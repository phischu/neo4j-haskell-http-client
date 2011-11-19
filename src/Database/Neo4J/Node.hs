{-# LANGUAGE OverloadedStrings, RecordWildCards #-}

module Database.Neo4J.Node where

import Database.Neo4J.Types
import Database.Neo4J.Internal
import Network.HTTP
import Network.URI
import Data.Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Attoparsec as Attoparsec
import Data.Text (Text)
import Data.ByteString.Class
import qualified Data.Vector as V
import Control.Monad
import Data.List.Split (splitOn)

-- | Create a node in Neo4J from a list of properties. The list may be empty.
createNode :: Client -> Properties -> IO (Either String Node)
createNode client properties = do
    let uri = serviceRootURI client `appendToPath` "node"
    let request = buildPost uri $ toStrictByteString $ encode $
                    object properties
    result <- simpleHTTP request
    return $ case result of
        Right response -> case pullReturnedSelfURI $ rspBody response of
            Just selfURI -> Right $ Node selfURI properties
            _            -> Left "no url returned :o"
        Left err -> Left $ show err

-- | Retrieve a node from its URI in Neo4J
getNode :: Client -> URI -> IO (Either String Node)
getNode client uri = clientGuard client [uri] $ do
    let request = mkRequest GET uri
    result <- simpleHTTP request
    return $ case result of
        Right response -> case rspCode response of
            (2, 0, 0) -> case nodeFromResponseBody $ rspBody response of
                Just node -> Right node
                _         -> Left "Node parse error"
            _ -> Left ("Node probably doesn't exist. Response code " ++
                        (show $ rspCode response))
        Left err -> Left $ show err

-- | Retrieve a node from its ID number in Neo4J
lookupNode :: Client -> NodeID -> IO (Either String Node)
lookupNode client nodeID = 
    getNode client $
        serviceRootURI client `appendToPath` "node"
            `appendToPath` (show nodeID)

deleteNode :: Client -> Node -> IO (Either String ())
deleteNode client node@(Node uri _) = clientGuard client [uri] $
    delete "Node" uri

-- | Retrieve nodes from their URIs in a batch operation
getNodes :: Client -> [URI] -> IO (Either String [Node])
getNodes client uris = clientGuard client uris $ do
        let urisWithIDs = zip [(0 :: Int)..] uris
        let request = mkBatchGETRequest urisWithIDs
        -- "responses are guaranteed to be returned in the same order the job
        -- descriptions are received."
        -- http://docs.neo4j.org/chunked/snapshot/rest-api-batch-ops.html
        result <- simpleHTTP request
        return $ case result of
            Right response -> case rspCode response of
                (2, 0, 0) -> case Attoparsec.parse json $ rspBody response of
                    Attoparsec.Done _ r ->
                        case (Aeson.parse parseBatchNodes r) of
                            Error err -> Left err
                            Success nodes -> Right nodes
                    Attoparsec.Fail _ contexts err ->
                        Left (show (contexts, err))
                _         -> Left (show result)                
    where
        mkBatchGETRequest urisWithIDs = buildRequestWithContent POST batchURI
                                            content
            where
                content = toStrictByteString $ encode $ toJSON $
                    map (\(id_, uri) -> object [
                        "method" .= ("GET" :: Text),
                        "to" .= toJSON ("/node/" ++
                                        (show $ getNodeIDFromURI uri)),
                        "id" .= id_]) urisWithIDs
        batchURI = serviceRootURI client `appendToPath` "batch"
        parseBatchNodes (Array a) = mapM parseBatchNode $ V.toList a
            where
                parseBatchNode :: Value -> Aeson.Parser Node
                parseBatchNode (Object o) = parseJSON =<< (o .: "body")
        parseBatchNodes _ = mzero
        getNodeIDFromURI :: URI -> NodeID
        getNodeIDFromURI uri = case splitOn "/" $ uriPath uri of
            [] -> error ("No node id in  " ++ (show uri))
            xs -> read $ last xs

nodeFromResponseBody body = do
    case Attoparsec.parse json body of
        Attoparsec.Done _ r -> case fromJSON r of
            Error err -> Nothing
            Success n@(Node {..}) -> Just n
        Attoparsec.Fail _ _ _ -> Nothing
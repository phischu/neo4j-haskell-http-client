{-# LANGUAGE OverloadedStrings #-}

module Database.Neo4J (
    Client, mkClient, defaultClient, defaultPort,
    nodeProperties, relationshipFrom, relationshipTo, relationshipType, relationshipProperties,
    getNodeID, createNode, getNode, lookupNode, deleteNode, createRelationship,
    deleteRelationship, getRelationships, incomingRelationships,
    outgoingRelationships,allRelationships, typedRelationships
    ) where

import Control.Monad
import Text.Printf
import qualified Data.Map as Map
import qualified Data.Vector as V
import Data.Either.Unwrap
import Data.Aeson
import Data.Aeson.Parser
import qualified Data.Attoparsec as Attoparsec
import Database.Neo4J.Types
import Database.Neo4J.Internal
import Network.HTTP
import Network.URI
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BSL
import Data.ByteString.Class
import Data.Either.Unwrap
import Control.Applicative
import Debug.Trace
import Data.Maybe

traceShow' x = traceShow x x

buildPost uri content =
        Request { rqURI = uri, rqMethod = POST, rqHeaders = headers, rqBody = content }
    where
        headers =
            [mkHeader HdrContentType "application/json", mkHeader HdrContentLength contentLength]
        contentLength = show $ BSC.length content

createNode client properties = do
    let uri = serviceRootURI client `appendToPath` "node"
    let request = buildPost uri $ toStrictByteString $ encode $ object properties
    result <- simpleHTTP request
    return $ case result of
        Right response -> case pullReturnedSelfURI $ rspBody response of
            Just selfURI -> Right $ Node selfURI properties
            _            -> Left "no url returned :o"
        Left err -> Left $ show err

getNode uri = do
    let request = mkRequest GET uri
    result <- simpleHTTP request
    return $ case result of
        Right response -> case rspCode response of
            (2, 0, 0) -> case pullReturnedSelfURI $ rspBody response of
                Just selfURI -> Right $ Node selfURI
                                    (fromMaybe [] $ pullNodeProperties $ rspBody response)
                _            -> Left "no url returned :o"
            _ -> Left ("Node probably doesn't exist. Response code " ++ (show $ rspCode response))
        Left err -> Left $ show err

lookupNode client nodeID = 
    getNode $ serviceRootURI client `appendToPath` "node" `appendToPath` (show nodeID)

deleteNode node@(Node uri _) = delete "Node" uri

createRelationship n@(Node from _) m@(Node to _) name = do
    let uri = from `appendToPath` "relationships"
    let request = buildPost uri $ toStrictByteString $ encode $
                    object [("to", toJSON $ show to), ("type", toJSON name)]
    result <- simpleHTTP request
    return $ case result of
        Right response -> case rspCode response of
            (2, 0, 1) -> case pullReturnedSelfURI $ rspBody response of
                Just selfURI -> Right $ Relationship selfURI n m name []
                _            -> Left "no url returned :o"
            _ -> Left ("Relationship wasn't created. See response: " ++ (show response))
        Left err -> Left $ show err

deleteRelationship (Relationship uri _ _ _ _) = delete "Relationship" uri

data RelationshipRetrievalType = All | Incoming | Outgoing | Typed String deriving (Eq)

getRelationships rrType (Node nodeURI _) = do
    let uri = nodeURI `appendToPath` "relationships" `appendToPath` case rrType of
            All ->  "all"
            Incoming -> "in"
            Outgoing -> "out"
            Typed relType -> "all/" ++ relType
    result <- simpleHTTP $ mkRequest GET uri
    let relationshipResult = case result of
            Right response -> case rspCode response of
                (2, 0, 0) -> let body = rspBody response in case Attoparsec.parse json body of
                    Attoparsec.Done _ (Array v) -> Right $ forM (V.toList v) $ \(Object o) -> do
                        start <- (parseURI <=< fromJSON') =<< Map.lookup "start" o
                        end <- (parseURI <=< fromJSON') =<< Map.lookup "end" o
                        self <- (parseURI <=< fromJSON') =<< Map.lookup "self" o
                        name <- fromJSON' =<< Map.lookup "type" o
                        let props = fromMaybe [] $ pullRelationshipProperties body
                        return (self, start, end, name, props)
                    _ -> Left ("Couldn't parse response " ++ (show response))
                _ -> Left $ show response
            Left err -> Left $ show err
    case relationshipResult of
        Right (Just xs) -> fmap sequence $ forM xs $ \(self, start, end, name, props) -> do
            x <- getNode start
            y <- getNode end
            case (x, y) of
                (Right x', Right y')   -> return $ Right $ Relationship self x' y' name props
                (Left err, Right _)    -> return $ Left $ printf "%s: %s" (show start) (show err)
                (Right _, Left err)    -> return $ Left $ printf "%s: %s" (show end) (show err)
                (Left err1, Left err2) -> return $ Left $ printf "%s: %s" (show (start, end))
                                                                          (show (err1, err2))
        Right _ -> return $ Left $ printf "Couldn't extract data from relationship response %s" $
                            show relationshipResult
        Left err -> return $ Left $ show err

incomingRelationships = getRelationships Incoming
outgoingRelationships = getRelationships Outgoing
allRelationships = getRelationships All
typedRelationships relType = getRelationships (Typed relType)
module Database.Neo4J.Node where

import Database.Neo4J.Types
import Database.Neo4J.Internal
import Network.HTTP
import Network.URI
import Data.Aeson
import Data.ByteString.Class

-- | Create a node in Neo4J from a list of properties. The list may be empty.
createNode :: Client -> Properties -> IO (Either String Node)
createNode client properties = do
    let uri = serviceRootURI client `appendToPath` "node"
    let request = buildPost uri $ toStrictByteString $ encode $ object properties
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
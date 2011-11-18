{-# LANGUAGE OverloadedStrings #-}

-- Hmm… so not all functions really need to get the client passed in, but for consistency's (and safety's on some level) sake, they all do in the module

{- |

Client interface to Neo4J over REST. Here's a simple example (never mind the cavalier pattern matching):

> import Database.Neo4J
>
> main = do
>    let client = mkClient "192.168.56.101" defaultPort
>    Right n <- createNode client [("name", "Neo"), ("quote", "Whoa.")]
>    Right m <- createNode client [("name", "Trinity"), ("quote", "Dodge this.")]
>    Right r <- createRelationship client n m "LOVES"
>    indexNodeByAllProperties client n
>    nodes <- findNodes client "name" "name" "Neo"
-}

-- module Database.Neo4J (
--     -- * Connecting to Neo4J
--     Client, mkClient, defaultClient, defaultPort,
--     -- * Working with Nodes
--     Node(), createNode, nodeProperties, getNodeID, getNode, lookupNode, deleteNode,
--     -- * Working with Relationships
--     Relationship (), relationshipFrom, relationshipTo, relationshipType,
--     relationshipProperties, createRelationship, updateRelationshipProperties,
--     deleteRelationship, getRelationship, getRelationships, incomingRelationships,
--     outgoingRelationships, allRelationships, typedRelationships,
--     -- * Working with Indexes
--     indexNode, indexNodeByProperty, indexNodeByAllProperties, findNodes,
--     -- * Data Types
--     Properties, Type, IndexName, RelationshipRetrievalType (..), NodeID
--     -- * Traversals
--     
--     ) where

module Database.Neo4J where

import Control.Monad
import Text.Printf
import qualified Data.Map as Map
import qualified Data.Vector as V
import Data.Either.Unwrap
import Data.Aeson
import Data.Aeson.Parser
import qualified Data.Attoparsec as Attoparsec
import Database.Neo4J.Node
import Database.Neo4J.Relationship
import Database.Neo4J.Types
import Database.Neo4J.Internal
import Database.Neo4J.Traversal
import Network.HTTP
import Network.URI
import Data.String
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.ByteString.Class
import Data.Either.Unwrap
import Control.Applicative
import Debug.Trace
import Data.Maybe

-- | Get relationships of a specified type
typedRelationships :: Client -> RelationshipType -> Node -> IO (Either String [Relationship])
typedRelationships client relType =
    getRelationships client (RetrieveTyped relType)

createNodeIndex :: Client -> IndexName -> IO (Either String ())
createNodeIndex client indexName = do
    let uri = serviceRootURI client `appendToPath` "index" `appendToPath`
            "node"
    let request = buildPost uri $ toStrictByteString $ encode $
            object [("name", toJSON indexName)]
    result <- simpleHTTP request
    return $ case result of
        Right response -> case rspCode response of
            (2, 0, 1) -> Right ()
            _         -> Left ("Index not created. Actual response: " ++
                                (show response))
        Left err -> Left (show err)

-- | If a node has a specific property, add the property to the given index.
indexNodeByProperty :: Client -> IndexName -> Text -> Node -> IO (Either String ())
indexNodeByProperty client indexName propertyName node@(Node nodeURI properties) = 
    case lookup propertyName properties of
        Just propertyValue -> indexNode client indexName node propertyName
                                propertyValue
        Nothing -> return $ Left $ printf "Property %s not in node %s"
                                        (Text.unpack propertyName) (show node)

-- | Add all properties of a given node to indices, each of which is named the same as the property key
indexNodeByAllProperties :: Client -> Node -> IO (Either String ())
indexNodeByAllProperties client node@(Node _ properties) = let keys = map fst properties in do
    rs <- mapM (\key -> indexNodeByProperty client (Text.unpack key) key node)
            keys
    return $ sequence_ rs

-- | Add a node to the given index, specifying your own key and value
indexNode :: Client -> IndexName -> Node -> Text -> Value -> IO (Either String ())
indexNode client indexName node@(Node nodeURI _) key value =
    case fromJSON' value of
        Just value' -> do
            let uri = serviceRootURI client `appendToPath`
                        "index" `appendToPath`
                        "node" `appendToPath` indexName `appendToPath`
                        (Text.unpack key) `appendToPath` value'
            result <- simpleHTTP $ buildPost uri $ toStrictByteString $
                encode $ toJSON $ show nodeURI
            return $ case fmap rspCode result of
                Right (2, 0, 1) -> Right ()
                Left err        -> Left $ show err
                _               ->
                    Left $ printf "Node not indexed. Actual response: %s"
                                (show result)
        Nothing -> return $ Left $
            printf "The impossible happened: %s couldn't be converted from JSON" (show value)

-- | The new way in > 1.5 MILESTONE (https://github.com/neo4j/community/issues/25;cid=1317413794432-668)
indexNodeNew :: (ToJSON a) => Client -> IndexName -> Node -> Text -> a -> IO (Either String ())
indexNodeNew client indexName node@(Node nodeURI _) key value = do
    let uri = serviceRootURI client `appendToPath` "index" `appendToPath` "node" `appendToPath`
            indexName
    print uri
    result <- simpleHTTP $ buildPost uri $ toStrictByteString $ encode $
        object [("uri", toJSON $ show nodeURI), ("key", toJSON key),
                ("value", toJSON value)]
    return $ case fmap rspCode result of
        Right (2, 0, 1) -> Right ()
        Left err        -> Left $ show err
        _               ->
            Left $
                printf "Node not indexed for some reason. Actual response: %s"
                    (show result)

-- | Search the given index for nodes whose keys have the specified value
findNodes :: Client -> IndexName -> Text -> String -> IO (Either String [Node])
findNodes client indexName key value = do
        let uri = serviceRootURI client `appendToPath` "index" `appendToPath`
                "node" `appendToPath` indexName `appendToPath`
                (Text.unpack key) `appendToPath` value
        result <- simpleHTTP $ mkRequest GET uri
        let node = case result of
                Right response -> case rspCode response of
                    (2, 0, 0) -> let body = rspBody response in
                        case Attoparsec.parse json body of
                            Attoparsec.Done _ (Array v) -> Right $
                                forM (V.toList v) $ \n -> 
                                    case n of
                                        Object o -> extractNode o
                                        _ -> Nothing
                            _ -> Left "findNode: Node parse error"
                    _ -> Left ("Node probably doesn't exist. Response code "
                        ++ (show $ rspCode response))
                Left err -> Left $ show err
        return $ case node of
            Right (Just n) -> Right n
            Right Nothing ->
                Left "Some weird node parse error deep in the code..."
            Left err -> Left err
    where
        extractNode o = do
            selfURIFromJSON <- fmap fromJSON $ Map.lookup "self" o
            selfURI <- case selfURIFromJSON of
                (Success possibleURI) -> parseURI possibleURI
                _                     -> Nothing
            nodePropertiesFromJSON <- fmap fromJSON $ Map.lookup "data" o
            nodeProperties <- case nodePropertiesFromJSON of
                (Success (Object propMap)) -> Just $ Map.toList propMap
                _                          -> Nothing
            return $ Node selfURI nodeProperties
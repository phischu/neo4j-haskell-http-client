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
>    print n
>    print m
>    Right r <- createRelationship n m "LOVES"
>    print r
>    indexNodeByAllProperties n
>    nodes <- findNodes client "name" "name" "Neo"
>    print nodes
-}

module Database.Neo4J (
    -- * Connecting to Neo4J
    Client, mkClient, defaultClient, defaultPort,
    -- * Working with Nodes
    Node(), createNode, nodeProperties, getNodeID, getNode, lookupNode, deleteNode,
    -- * Working with Relationships
    Relationship (), relationshipFrom, relationshipTo, relationshipType,
    relationshipProperties, createRelationship, updateRelationshipProperties,
    deleteRelationship, getRelationship, getRelationships, incomingRelationships,
    outgoingRelationships, allRelationships, typedRelationships,
    -- * Working with Indexes
    indexNode, indexNodeByProperty, indexNodeByAllProperties, findNodes,
    -- * Data Types
    Properties, Type, IndexName, RelationshipRetrievalType (..), NodeID
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
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.ByteString.Class
import Data.Either.Unwrap
import Control.Applicative
import Debug.Trace
import Data.Maybe
import Data.List.Split (splitOn)

data RelationshipRetrievalType = All | Incoming | Outgoing | Typed String deriving (Eq)

nodeFromResponseBody body = do
    selfURI <- pullReturnedSelfURI body
    return $ Node selfURI (fromMaybe [] $ pullNodeProperties body)

buildRequestWithContent method uri content =
        Request { rqURI = uri, rqMethod = method, rqHeaders = headers, rqBody = content }
    where
        headers =
            [mkHeader HdrContentType "application/json", mkHeader HdrContentLength contentLength]
        contentLength = show $ BSC.length content

buildPut = buildRequestWithContent PUT
buildPost = buildRequestWithContent POST

checkClient client uris = all (\uri -> dbInfo (serviceRootURI client) == dbInfo uri) uris
    where
        -- only for comparing equality!
        -- example:
        -- `dbInfo "http://192.168.56.101:7474/db/data/relationship/17"` =>
        -- `["http:","","192.168.56.101:7474","db","data"]`
        dbInfo = take 5 . splitOn "/" . show

clientGuard :: Client -> [URI] -> IO (Either String a) -> IO (Either String a)
clientGuard client uris f
    | checkClient client uris =
        return $ Left "Neo4j data must come from the same client"
    | otherwise = f

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
            _ -> Left ("Node probably doesn't exist. Response code " ++ (show $ rspCode response))
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

-- | Create a relationship between two nodes. You must specify a name/relationship type
createRelationship :: Client -> Node -> Node -> String -> Properties -> IO (Either String Relationship)
createRelationship client n@(Node from _) m@(Node to _) name properties =
    clientGuard client [from, to] $ do
        let uri = from `appendToPath` "relationships"
        let request = buildPost uri $ toStrictByteString $ encode $
                    object [("to", toJSON $ show to), ("type", toJSON name)]
        r <- getRelationshipFromHTTPResult client =<< simpleHTTP request
        case (properties, r) of
            ([], _)            -> return r
            (_, Right relationship) ->
                updateRelationshipProperties client relationship properties
            (_, Left err)           -> return $ Left err

getRelationshipFromHTTPResult client result = case result of
    Right response -> case rspCode response of
        (2, 0, 0) -> let body = rspBody response in
            case Attoparsec.parse json body of
                Attoparsec.Done _ o ->
                    case relationshipAttributesFromParsedResponse body o of
                    Just attrs  -> mkRelationshipFromAttributes client attrs
                    Nothing     -> return $
                        Left "Couldn't pull relationship attributes"
                _            -> return $ Left
                    ("Couldn't parse response as JSON. Response : " ++
                                        (show response))
        (2, 0, 1) -> case pullReturnedSelfURI $ rspBody response of
            Just selfURI -> getRelationship client selfURI
            _            -> return $ Left "no url returned :o"
        _ -> return $
            Left ("Relationship wasn't returned in response. " ++
                (show response))
    Left err -> return $ Left $ show err

getRelationship :: Client -> URI -> IO (Either String Relationship)
getRelationship client uri = clientGuard client [uri] $ do
    let request = mkRequest GET uri
    (getRelationshipFromHTTPResult client) =<< simpleHTTP request

-- | Update the properties of a relationship
updateRelationshipProperties :: Client -> Relationship -> Properties -> IO (Either String Relationship)
updateRelationshipProperties client r ps =
    clientGuard client [(relationshipURI r)] $ do
        let uri = (relationshipURI r) `appendToPath` "properties"
        let request = buildPut uri (toStrictByteString $ encode $ object ps)
        result <- simpleHTTP request
        case result of
            Right response -> case rspCode response of
                (2, 0, 4) -> getRelationship defaultClient (relationshipURI r)
                _         -> return $ Left ("Couldn't update relationship. "
                                ++ (show response))

deleteRelationship :: Client -> Relationship -> IO (Either String ())
deleteRelationship client (Relationship uri _ _ _ _) =
    clientGuard client [uri] $ delete "Relationship" uri

relationshipAttributesFromParsedResponse body (Object o) = do
    start <- (parseURI <=< fromJSON') =<< Map.lookup "start" o
    end <- (parseURI <=< fromJSON') =<< Map.lookup "end" o
    self <- (parseURI <=< fromJSON') =<< Map.lookup "self" o
    name <- fromJSON' =<< Map.lookup "type" o
    props <- (fmap Map.toList . fromJSON') =<< Map.lookup "data" o
    return (self, start, end, name, props)
relationshipAttributesFromParsedResponse _ _ = Nothing

mkRelationshipFromAttributes client (self, start, end, name, props) = do
    x <- getNode client start
    y <- getNode client end
    case (x, y) of
        (Right x', Right y')   ->
            return $ Right $ Relationship self x' y' name props
        (Left err, Right _)    ->
            return $ Left $ printf "%s: %s" (show start) (show err)
        (Right _, Left err)    ->
            return $ Left $ printf "%s: %s" (show end) (show err)
        (Left err1, Left err2) ->
            return $ Left $ printf "%s: %s" (show (start, end))
                                            (show (err1, err2))

-- | Get all the relationships of a given type for a node.
getRelationships :: Client -> RelationshipRetrievalType -> Node -> IO (Either String [Relationship])
getRelationships client rrType (Node nodeURI _) = do
    let uri = nodeURI `appendToPath` "relationships" `appendToPath`
            case rrType of
                All ->  "all"
                Incoming -> "in"
                Outgoing -> "out"
                Typed relType -> "all/" ++ relType
    result <- simpleHTTP $ mkRequest GET uri
    let relationshipResult = case result of
            Right response -> case rspCode response of
                (2, 0, 0) -> let body = rspBody response in
                    case Attoparsec.parse json body of
                        Attoparsec.Done _ (Array v) -> Right $ mapM 
                            (relationshipAttributesFromParsedResponse body)
                                (V.toList v)
                        _ -> Left ("Couldn't parse response " ++
                                (show response))
                _ -> Left $ show response
            Left err -> Left $ show err
    case relationshipResult of
        Right (Just xs) -> fmap sequence $
            mapM (mkRelationshipFromAttributes client) xs
        Right _ -> return $ Left $
            printf "Couldn't extract data from relationship response %s" $
                    show relationshipResult
        Left err -> return $ Left $ show err

incomingRelationships client = getRelationships client Incoming
outgoingRelationships client = getRelationships client Outgoing
allRelationships client = getRelationships client All

-- | Get relationships of a specified type
typedRelationships :: Client -> RelationshipType -> Node -> IO (Either String [Relationship])
typedRelationships client relType = getRelationships client (Typed relType)

createNodeIndex :: Client -> IndexName -> IO (Either String ())
createNodeIndex client indexName = do
    let uri = serviceRootURI client `appendToPath` "index" `appendToPath` "node"
    let request = buildPost uri $ toStrictByteString $ encode $
            object [("name", toJSON indexName)]
    result <- simpleHTTP request
    return $ case result of
        Right response -> case rspCode response of
            (2, 0, 1) -> Right ()
            _         -> Left ("Index not created. Actual response: " ++ (show response))
        Left err -> Left (show err)

-- | If a node has a specific property, add the property to the given index.
indexNodeByProperty :: Client -> IndexName -> Text -> Node -> IO (Either String ())
indexNodeByProperty client indexName propertyName node@(Node nodeURI properties) = 
    case lookup propertyName properties of
        Just propertyValue -> indexNode client indexName node propertyName propertyValue
        Nothing -> return $ Left $ printf "Property %s not in node %s"
                                        (Text.unpack propertyName) (show node)

-- | Add all properties of a given node to indices, each of which is named the same as the property key
indexNodeByAllProperties :: Client -> Node -> IO (Either String ())
indexNodeByAllProperties client node@(Node _ properties) = let keys = map fst properties in do
    rs <- mapM (\key -> indexNodeByProperty client (Text.unpack key) key node) keys
    return $ sequence_ rs

-- | Add a node to the given index, specifying your own key and value
indexNode :: Client -> IndexName -> Node -> Text -> Value -> IO (Either String ())
indexNode client indexName node@(Node nodeURI _) key value =
    case fromJSON' value of
        Just value' -> do
            let uri = serviceRootURI client `appendToPath` "index" `appendToPath`
                        "node" `appendToPath` indexName `appendToPath`
                        (Text.unpack key) `appendToPath` value'
            result <- simpleHTTP $ buildPost uri $ toStrictByteString $ encode $
                toJSON $ show nodeURI
            return $ case fmap rspCode result of
                Right (2, 0, 1) -> Right ()
                Left err        -> Left $ show err
                _               -> Left $ printf "Node not indexed. Actual response: %s"
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
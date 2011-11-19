{-# LANGUAGE OverloadedStrings #-}

module Database.Neo4j.Types where

import Text.Printf
import Network.URI
import Network.Socket
import qualified Data.Text as Text
import Data.Aeson
import Data.Aeson.Types
import Data.List.Split
import qualified Data.Map as Map

data Client = Client {
    host :: HostName,
    port :: ServiceName,
    serviceRootURI :: URI
} deriving (Show)

type NodeID = Integer

data Node = Node {
    nodeURI :: URI,
    nodeProperties :: Properties
} deriving (Eq, Show)

type Properties = [Pair]

data Relationship = Relationship {
    relationshipURI :: URI,
    -- | Get the node that the relationship originates from
    relationshipFrom :: Node,
    -- | Get the node at which the relationship ends
    relationshipTo :: Node,
    -- | Get the type of the relationship
    relationshipType :: RelationshipType,
    -- | Get the properties associated with this relationship
    relationshipProperties :: Properties
} deriving (Eq, Show)

data Path = Path {
    pathStart :: Node,
    pathNodes :: [Node],
    pathLength :: Integer,
    pathRelationships :: [Relationship],
    pathEnd :: Node
} deriving (Eq, Show)

type IndexName = String
type RelationshipType = String

instance ToJSON Node where
    toJSON (Node _ properties) = object properties

instance FromJSON Node where
    parseJSON (Object o) = do
        selfURI <- parseURI' =<< (o .: "self")
        properties <- parseProperties =<< (o .: "data")
        return $ Node selfURI properties

parseURI' x = case parseURI x of
    Nothing -> fail ("URI " ++ x ++ " is unparseable")
    Just x -> return x

parseProperties (Object o) = return $ Map.toList o
parseProperties _ = fail "properties couldn't be parsed"

instance ToJSON Relationship where
    toJSON (Relationship uri x y t properties) =
        object ["to" .= y, "type" .= t]

getNodeID :: Node -> NodeID
getNodeID n@(Node uri _) = case splitOn "/" $ uriPath uri of
    [] -> error ("No node id for node " ++ (show n))
    xs -> read $ last xs

mkClient :: HostName -> ServiceName -> Client
mkClient hostname portNumber = c { serviceRootURI = buildServiceRootURI c }
    where
        c = Client { host = hostname, port = portNumber,
                     serviceRootURI = nullURI }
        buildServiceRootURI client = nullURI {
            uriScheme = "http:",
            uriAuthority = Just URIAuth {
                uriUserInfo = "", uriRegName = host client,
                uriPort = ":" ++ (port client)
            },
            uriPath = "/db/data"
        }

-- | Port 7474
defaultPort :: ServiceName
defaultPort = "7474"

-- | Client over localhost on port 7474
defaultClient :: Client
defaultClient = mkClient "localhost" defaultPort
{-# LANGUAGE OverloadedStrings #-}

module Database.Neo4J.Types where

import Text.Printf
import Network.URI
import Network.Socket
import qualified Data.Text as Text
import Data.Aeson
import Data.Aeson.Types
import Data.List.Split

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
    relationshipFrom :: Node,
    relationshipTo :: Node,
    relationshipType :: RelationshipType,
    relationshipProperties :: Properties
} deriving (Eq, Show)

data Path = Path {
    pathLength :: Integer,
    pathStart :: Node,
    pathNodes :: [Node],
    pathRelationships :: [Relationship],
    pathEnd :: Node
} deriving (Eq, Show)

type IndexName = String
type RelationshipType = String

instance ToJSON Node where
    toJSON (Node _ properties) = object properties

instance ToJSON Relationship where
    toJSON (Relationship uri x y t properties) = object ["to" .= y, "type" .= t]

getNodeID :: Node -> NodeID
getNodeID n@(Node uri _) = case splitOn "/" $ uriPath uri of
    [] -> error ("No node id for node " ++ (show n))
    xs -> read $ last xs

mkClient :: HostName -> ServiceName -> Client
mkClient hostname portNumber = c { serviceRootURI = buildServiceRootURI c}
    where
        c = Client { host = hostname, port = portNumber, serviceRootURI = nullURI }
        buildServiceRootURI client = nullURI {
            uriScheme = "http:",
            uriAuthority = Just URIAuth {
                uriUserInfo = "", uriRegName = host client, uriPort = ":" ++ (port client)
            },
            uriPath = "/db/data"
        }

-- | Port 7474
defaultPort :: ServiceName
defaultPort = "7474"

-- | Client over localhost on port 7474
defaultClient :: Client
defaultClient = mkClient "localhost" defaultPort
{-# LANGUAGE RecordWildCards #-}

module Database.Neo4J.Traversal where

import Prelude hiding (catch)
import Database.Neo4J.Node
import Database.Neo4J.Relationship
import Database.Neo4J.Internal
import Database.Neo4J.Types
import Data.Aeson
import Data.Aeson.Types hiding (parse)
import qualified Data.Vector as V
import Data.Attoparsec
import Network.HTTP hiding (Done)
import Network.URI
import Data.String
import Data.ByteString.Class
import Data.List
import Data.Maybe
import Data.Function
import Control.Monad
import Control.Monad.Trans.Either
import Control.Applicative
import System.IO.Unsafe
import Control.Exception

type TraversalOption = Pair

data TraversalReturnType = ReturnNodes | ReturnRelationships | ReturnPaths | ReturnFullPaths deriving (Eq)

instance Show TraversalReturnType where
    show ReturnNodes = "node"
    show ReturnRelationships = "relationship"
    show ReturnPaths = "path"
    show ReturnFullPaths = "full_path"

instance ToJSON TraversalReturnType where
    toJSON = toJSON . show

data TraversalOrder = BFS | DFS deriving (Eq)

instance Show TraversalOrder where
    show BFS = "breadth_first"
    show DFS = "depth_first"

instance ToJSON TraversalOrder where
    toJSON = toJSON . show

type TraverseRelationships = [TraverseRelationship]

data TraverseRelationship = TraverseAll RelationshipType | TraverseIncoming RelationshipType | TraverseOutgoing RelationshipType deriving (Eq)

instance ToJSON TraverseRelationship where
    toJSON (TraverseAll rType) = buildTraversalRelationshipObj "all" rType
    toJSON (TraverseIncoming rType) = buildTraversalRelationshipObj "in" rType
    toJSON (TraverseOutgoing rType) =
        buildTraversalRelationshipObj "out" rType

buildTraversalRelationshipObj dir rType = 
    object [fromString "direction" .= dir, fromString "type" .= rType]

data TraversalUniqueness = NodeGlobalUniqueness | RelationshipGlobalUniqueness | NodePathUniqueness | RelationshipPathUniqueness | NoUniqueness deriving (Eq)

instance ToJSON TraversalUniqueness where
    toJSON NodeGlobalUniqueness = toJSON "node_global"
    toJSON RelationshipGlobalUniqueness = toJSON "relationship_global"
    toJSON NodePathUniqueness = toJSON "node_path"
    toJSON RelationshipPathUniqueness = toJSON "relationship_path"
    toJSON NoUniqueness = toJSON "none"

data Language = JavaScript | BuiltIn deriving (Eq)

instance ToJSON Language where
    toJSON BuiltIn = toJSON "builtin"
    toJSON JavaScript = toJSON "javascript"

data TraversalReturnFilter = CustomReturnFilter String Language | ReturnAll | ReturnAllButStartNode deriving (Eq)

instance ToJSON TraversalReturnFilter where
    toJSON ReturnAll = object
        [fromString "name" .= "all", fromString "language" .= BuiltIn]
    toJSON ReturnAllButStartNode =
        object [fromString "name" .= "all_but_start_node",
                fromString "language" .= BuiltIn]
    toJSON (CustomReturnFilter code lang) =
        object [fromString "body" .= code, fromString "language" .= lang]

data TraversalPruneEvaluator = PruneEvaluator String | NoEvaluator deriving (Eq)

instance ToJSON TraversalPruneEvaluator where
    toJSON _ = object
        [fromString "name" .= "none", fromString "language" .= BuiltIn]

data PathWithURIs = PathWithURIs {
    prStart :: URI,
    prNodes :: [URI],
    prLength :: Integer,
    prRelationships :: [URI],
    prEnd :: URI
} deriving (Show, Eq)

data PathsWithURIs = PathsWithURIs [PathWithURIs]

instance FromJSON PathsWithURIs where
    parseJSON (Array a) = PathsWithURIs <$> mapM parsePFR (V.toList a)
    parseJSON _ = mzero

parsePFR (Object o) = do
        start <- parseURI' =<< o .: fromString "start"
        nodes <- (mapM parseURI') =<< o .: fromString "nodes"
        len <- o .: fromString "length"
        relationships <- (mapM parseURI') =<<
            o .: fromString "relationships"
        end <-  parseURI' =<< o .: fromString "end"
        return $ PathWithURIs start nodes len relationships end
    where
        fromJSONToInt :: Value -> Maybe Int
        fromJSONToInt x = case fromJSON x of
            Error err -> Nothing
            Success x -> Just x
        parseURI' text = case parseURI text of
            Just x -> return x
            Nothing -> fail (show text ++ " isn't a parseable URI")

order :: TraversalOrder -> TraversalOption
order x = fromString "order" .= x

uniqueness :: TraversalUniqueness -> TraversalOption
uniqueness x = fromString "uniqueness" .= x

returnFilter :: TraversalReturnFilter -> TraversalOption
returnFilter x = fromString "return_filter" .= x

maxDepth :: Int -> TraversalOption
maxDepth x = fromString "max_depth" .= x

pruneEvaluator :: TraversalPruneEvaluator -> TraversalOption
pruneEvaluator x = fromString "prune_evaluator" .= x

relationships :: TraverseRelationships -> TraversalOption
relationships xs = fromString "relationships" .= xs

defaultBFSOptions =
    [order BFS, uniqueness NodeGlobalUniqueness,
     pruneEvaluator NoEvaluator, returnFilter ReturnAll, maxDepth 3]

-- | Favours second for what ends up in the result (i.e. second keys overwrite first)
set :: [Pair] -> [Pair] -> [Pair]
set xs ys = nubBy ((==) `on` fst) (ys ++ xs)

traverse traversalOptions returnType (Node nodeURI _) = EitherT $ do
    let uri = nodeURI `appendToPath` "traverse" `appendToPath`
            (show returnType)
    let request = buildPost uri $ toStrictByteString $
            encode $ object traversalOptions
    print request
    result <- simpleHTTP request
    return $ case result of
        Right response -> case rspCode response of
            (2, 0, 0) -> Right $ rspBody response
            _         -> Left ("shit happened: " ++ (show response))
        Left err -> Left $ show err

pathTraversalURIs :: Client -> [TraversalOption] -> Node -> IO (Either String [PathWithURIs])
pathTraversalURIs client traversalOptions node = runEitherT $ do
    traverseResult <- traverse traversalOptions ReturnPaths node
    EitherT $ return $ case parse json traverseResult of
        Done _ r -> case fromJSON r of
            Error err -> Left err
            Success (PathsWithURIs ps) -> Right ps
        Fail _ contexts err -> Left (show (contexts, err))

pathTraversal :: Client -> [TraversalOption] -> Node -> IO (Either String [Path])
pathTraversal client traversalOptions node = runEitherT $ do
    pfrs <- EitherT $ pathTraversalURIs client traversalOptions node
    dereferencePaths client pfrs

tryAndRetry f = catch f (\e -> print (e :: IOException) >> f)

dereferencePaths client pfrs = mapM deref pfrs
    where
        deref (PathWithURIs {..}) = do
            start <- getNode' client prStart
            nodes <- mapM (getNode' client) prNodes
            let len = prLength
            relationships <- mapM (getRelationship' client) prRelationships
            end <- getNode' client prEnd
            return $ Path len start nodes relationships end
        getNode' client uri = EitherT $ unsafeInterleaveIO $
            getNode client uri
        getRelationship' client uri = EitherT $ unsafeInterleaveIO $
            getRelationship client uri
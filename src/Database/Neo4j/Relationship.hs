{-# LANGUAGE OverloadedStrings #-}

module Database.Neo4j.Relationship where

import Database.Neo4j.Node
import qualified Data.HashMap.Lazy as Map
import Database.Neo4j.Types
import Database.Neo4j.Internal
import Network.HTTP
import Network.URI
import Data.Aeson
import Text.Printf
import qualified Data.Attoparsec as Attoparsec
import Control.Monad
import qualified Data.Vector as V

getRelationshipFromHTTPResult client result = case result of
    Right response -> case rspCode response of
        (2, 0, 0) -> let body = rspBody response in
            case Attoparsec.parse json body of
                Attoparsec.Done _ o ->
                    case relationshipAttributesFromParsedResponse o of
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

data RelationshipRetrievalType = RetrieveAll | RetrieveIncoming | RetrieveOutgoing | RetrieveTyped String deriving (Eq)

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

relationshipAttributesFromParsedResponse (Object o) = do
    start <- (parseURI <=< fromJSON') =<< Map.lookup "start" o
    end <- (parseURI <=< fromJSON') =<< Map.lookup "end" o
    self <- (parseURI <=< fromJSON') =<< Map.lookup "self" o
    name <- fromJSON' =<< Map.lookup "type" o
    props <- (fmap Map.toList . fromJSON') =<< Map.lookup "data" o
    return (self, start, end, name, props)
relationshipAttributesFromParsedResponse _ = Nothing

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
    let uri = case rrType of
                RetrieveAll -> nodeURI `appendToPath` "relationships" `appendToPath` "all"
                RetrieveIncoming -> nodeURI `appendToPath` "relationships" `appendToPath` "in"
                RetrieveOutgoing -> nodeURI `appendToPath` "relationships" `appendToPath` "out"
                RetrieveTyped relType -> nodeURI `appendToPath` "relationships" `appendToPath` "all" `appendToPath` relType
    result <- simpleHTTP $ mkRequest GET uri
    let relationshipResult = case result of
            Right response -> case rspCode response of
                (2, 0, 0) -> let body = rspBody response in
                    case Attoparsec.parse json body of
                        Attoparsec.Done _ (Array v) -> Right $ mapM
                            (relationshipAttributesFromParsedResponse)
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

incomingRelationships client = getRelationships client RetrieveIncoming
outgoingRelationships client = getRelationships client RetrieveOutgoing
allRelationships client = getRelationships client RetrieveAll

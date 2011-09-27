module Database.Neo4J.Internal where

import Control.Monad
import Data.Maybe
import Data.Aeson
import Data.Aeson.Parser
import qualified Data.Attoparsec as Attoparsec
import qualified Data.Map as Map
import Network.HTTP
import Network.HTTP.Base
import Network.URI
import qualified Data.ByteString.Char8 as BSC
import Debug.Trace

traceShow' x = traceShow x x

escape = escapeURIString isUnreserved

appendToPath uri appendage = uri { uriPath = uriPath uri ++ appendage' }
    where appendage' = case appendage of
                        [] -> []
                        ('/':_) -> appendage
                        _ -> '/':appendage

lookupAllInJSONResponse jsonResponse keys = case Attoparsec.parse json jsonResponse of
    Attoparsec.Done _ (Object o) -> sequence $ map (\key -> fmap fromJSON $ Map.lookup key o) keys
    x                            -> Nothing

fromJSON' x = case fromJSON x of
    Error err -> Nothing
    Success x -> Just x

lookupInJSONResponse jsonResponse key = join $ fmap listToMaybe $
    lookupAllInJSONResponse jsonResponse [key]

pullReturnedSelfURI jsonResponse = case lookupInJSONResponse jsonResponse "self" of
    Just (Success possibleURI) -> parseURI possibleURI
    _                          -> Nothing

pullNodeProperties jsonResponse = case lookupInJSONResponse jsonResponse "data" of
    Just (Success (Object propertiesMap)) -> Just $ Map.toList propertiesMap
    _                                     -> Nothing

pullRelationshipProperties = pullNodeProperties

delete name uri = do
    let request = mkRequest DELETE uri :: Request BSC.ByteString
    result <- simpleHTTP request
    return $ case result of
        Right response -> case rspCode response of
            (2, 0, 4) -> Right ()
            _ -> Left (name ++ " wasn't deleted. See response: " ++ (show response))
        Left err -> Left (show err)
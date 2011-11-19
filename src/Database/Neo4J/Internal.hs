{-# LANGUAGE OverloadedStrings, RecordWildCards #-}

module Database.Neo4j.Internal where

import Database.Neo4j.Types
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
import Data.List.Split (splitOn)

traceShow' x = traceShow x x

escape = escapeURIString isUnreserved

appendToPath uri appendage = uri { uriPath = uriPath uri ++ appendage' }
    where appendage' = case appendage of
                        [] -> []
                        ('/':_) -> escape appendage
                        _ -> '/':(escape appendage)

buildRequestWithContent method uri content =
        Request { rqURI = uri, rqMethod = method, rqHeaders = headers,
                  rqBody = content }
    where
        headers =
            [mkHeader HdrContentType "application/json",
             mkHeader HdrContentLength contentLength]
        contentLength = show $ BSC.length content

buildPut = buildRequestWithContent PUT
buildPost = buildRequestWithContent POST

lookupAllInJSONResponse jsonResponse keys = case Attoparsec.parse json jsonResponse of
    Attoparsec.Done _ (Object o) -> sequence $
        map (\key -> fmap fromJSON $ Map.lookup key o) keys
    _                            -> Nothing

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

clientGuard :: Client -> [URI] -> IO (Either String a) -> IO (Either String a)
clientGuard client uris f
    | not $ checkClient client uris =
        return $ Left "Neo4j data must come from the same client"
    | otherwise = f

checkClient client uris = all (\uri -> dbInfo (serviceRootURI client) == dbInfo uri) uris
    where
        -- only for comparing equality!
        -- example:
        -- `dbInfo "http://192.168.56.101:7474/db/data/relationship/17"` =>
        -- `["http:","","192.168.56.101:7474","db","data"]`
        dbInfo = take 5 . splitOn "/" . show
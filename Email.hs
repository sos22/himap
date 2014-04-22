{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Email(Email(Email, eml_body, eml_headers),
             Header(Header),
             parseEmail) where

import Control.Monad.Writer.Lazy
import qualified Data.ByteString as BS
import Data.Char
import Data.List
import Data.Word

import Util

data Header = Header String String deriving Show
data Email = Email { eml_headers :: [Header],
                     eml_body :: BS.ByteString }
             deriving Show

parseAscii7 :: [Word8] -> String
parseAscii7 = map $ chr . fromInteger . toInteger

{- Map from a bytestring to a list of (line, cont) pairs, where the
cont is the entire contents of the bytestring after that line. -}
lines :: BS.ByteString -> [([Word8], BS.ByteString)]
lines s =
  let splitIdx searchFrom =
        if searchFrom > BS.length s
        then Nothing
        else if BS.index s searchFrom == 10
             then Just (searchFrom, searchFrom + 1)
             else if searchFrom + 1 < BS.length s &&
                     BS.index s searchFrom == 13 &&
                     BS.index s (searchFrom + 1) == 10
                  then Just (searchFrom, searchFrom + 2)
                  else splitIdx $ searchFrom + 1
  in case splitIdx 0 of
    Nothing -> [(BS.unpack s, BS.empty)]
    Just (endline, startnext) ->
      let (a, b) = BS.splitAt endline s
          (_, d) = BS.splitAt (startnext - endline) b
      in (BS.unpack a, d):(Email.lines d)

headersBody :: BS.ByteString -> Errorable ([String], BS.ByteString)
headersBody s =
  let ss = Email.lines s in
  case find (\((x,_),_) -> x == []) (zip ss $ inits ss) of
    Nothing -> tell ["Message has no body?"] >> return ( (map (parseAscii7 . fst) ss), BS.empty)
    Just ((_blank, body), headers) -> return (map (parseAscii7 . fst) headers, body) 

unfoldHeaderLines :: [String] -> [String]
unfoldHeaderLines what =
  let isLinearWhiteSpace c = c `elem` " \t"
      worker :: [String] -> (Maybe String, [String])
      worker [] = (Nothing, [])
      worker ([]:others) = case worker others of
        (Nothing, res) -> (Nothing, res)
        (Just l, res) -> (Nothing, l:res)
      worker (ccs@(c:_):others) | isLinearWhiteSpace c =
        case worker others of
          (Nothing, res) -> (Just ccs, res)
          (Just r, res) -> (Just (ccs ++ r), res)
        | otherwise = case worker others of
          (Nothing, res) -> (Nothing, ccs:res)
          (Just e, res) -> (Nothing, (ccs ++ e):res)
  in case worker what of
    (Nothing, res) -> res
    (Just c, res) -> c:res

toSep :: Eq a => a -> [a] -> Maybe ([a], [a])
toSep sep elems =
  let stateSequence = zip elems $ zip (inits elems) (tails $ tail elems)
  in case find (\(a, _) -> a == sep) stateSequence of
    Nothing -> Nothing
    Just x -> Just $ snd x

extractHeader :: String -> Errorable Header
extractHeader hdrLine =
  case toSep ':' hdrLine of
    Nothing -> tell ["No : in header line " ++ hdrLine] >> (return $ Header hdrLine "")
    Just (a, b) -> return $ Header a (dropWhile (flip elem " \t") b)

parseEmail :: BS.ByteString -> Errorable Email
parseEmail what =
  do (headerLines, body) <- headersBody what
     parsedHeaders <- mapM extractHeader $ unfoldHeaderLines headerLines
     return $ Email {eml_headers = parsedHeaders, 
                     eml_body = body }


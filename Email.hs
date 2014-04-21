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

{- Nothing represents newlines -}
findNewlines :: BS.ByteString -> [(Maybe Word8, BS.ByteString)]
findNewlines bsl =
  let tl = BS.tail bsl
      hd = BS.head bsl
      hdtl = BS.head tl
      tltl = BS.tail tl
  in if BS.null bsl
     then []
     else if hd == 10
          then (Nothing, tl):(findNewlines tl)
          else if BS.null tl || hd /= 13 || hdtl /= 10
               then (Just hd, tl):(findNewlines tl)
               else (Nothing, tltl):(findNewlines tltl)

lines :: BS.ByteString -> [([Word8], BS.ByteString)]
lines s =
  let worker :: [Word8] -> [(Maybe Word8, BS.ByteString)] -> [([Word8], BS.ByteString)]
      worker fromPrevNewline remainder =
        case remainder of
          [] -> [(fromPrevNewline, BS.empty)]
          ((Nothing, trailer):rest) ->
            (fromPrevNewline, trailer):(worker [] rest)
          ((Just c, _):rest) ->
            worker (fromPrevNewline ++ [c]) rest
  in worker [] $ findNewlines s

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


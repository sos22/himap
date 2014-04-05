{-# LANGUAGE ScopedTypeVariables #-}
import Data.List
import Control.Monad.Writer.Lazy
import qualified Data.ByteString.Lazy as BSL
import Data.Word
import Data.Char

type Errorable = Writer String

stateSequence :: [a] -> [(a, ([a], [a]))]
stateSequence elems =
  zip elems $ zip (inits elems) (tails $ tail elems)

toSep :: Eq a => a -> [a] -> Maybe ([a], [a])
toSep sep elems =
  case find (\(a, _) -> a == sep) $ stateSequence elems of
    Nothing -> Nothing
    Just x -> Just $ snd x

{- Nothing represents newlines -}
findNewlines :: BSL.ByteString -> [(Maybe Word8, BSL.ByteString)]
findNewlines bsl =
  let tl = BSL.tail bsl
      hd = BSL.head bsl
      hdtl = BSL.head tl
      tltl = BSL.tail tl
  in if BSL.null bsl
     then []
     else if BSL.null tl
          then [(Just hd, BSL.empty)]
          else if or [BSL.null tltl, hd /= 13, hdtl /= 10]
               then (Just hd, tl):(findNewlines tl)
               else (Nothing, tltl):(findNewlines tltl)

lines :: BSL.ByteString -> [([Word8], BSL.ByteString)]
lines s =
  let worker :: [Word8] -> [(Maybe Word8, BSL.ByteString)] -> [([Word8], BSL.ByteString)]
      worker fromPrevNewline remainder =
        case remainder of
          [] -> [(fromPrevNewline, BSL.empty)]
          ((Nothing, trailer):rest) ->
            (fromPrevNewline, trailer):(worker [] rest)
          ((Just c, _):rest) ->
            worker (fromPrevNewline ++ [c]) rest
  in worker [] $ findNewlines s

headersBody :: BSL.ByteString -> Errorable ([String], BSL.ByteString)
headersBody s =
  let ss = Main.lines s in
  case find (\((x,_),_) -> x == []) (zip ss $ inits ss) of
    Nothing -> tell "Message has no body?" >> return ([],s)
    Just ((_blank, body), headers) -> return (map (map (chr . fromInteger . toInteger) . fst) headers, body) 

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

parseHeader :: String -> Errorable (String, String)
parseHeader hdrLine =
  case toSep ':' hdrLine of
    Nothing -> tell ("No : in header line " ++ hdrLine) >> return (hdrLine, "")
    Just (a, b) -> return (a, dropWhile (flip elem " \t") b)
  
parseEmail :: BSL.ByteString -> Errorable ([(String, String)], BSL.ByteString)
parseEmail what =
  do (headerLines, body) <- headersBody what
     parsedHeaders <- mapM parseHeader $ unfoldHeaderLines headerLines
     return (parsedHeaders, body)

stringToByteString :: String -> BSL.ByteString
stringToByteString = BSL.pack . map (fromInteger . toInteger . ord)

main :: IO ()
main =
  do print $ runWriter $ headersBody $ stringToByteString "H1\r\nH2\r\nH3\r\n\r\nbody"
     print $ unfoldHeaderLines $ ["L1", " C1", " C2", "L2", "L3", "L4", " C3"]
     print $ runWriter $ parseEmail $ stringToByteString "From: foo\r\n bar\r\n\tbazz\r\nTo: Me\r\n\r\nBody"


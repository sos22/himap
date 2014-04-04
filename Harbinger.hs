{-# LANGUAGE ScopedTypeVariables #-}
import Data.List
import Control.Monad.Writer.Lazy

first :: (a -> b) -> (a, c) -> (b, c)
first f (x, y) = (f x, y)

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
findNewlines :: [Char] -> [(Maybe Char, String)]
findNewlines [] = []
findNewlines [x] = [(Just x, [])]
findNewlines ('\r':'\n':x) = (Nothing, x):(findNewlines x)
findNewlines (x:y) = (Just x, y):(findNewlines y)

lines :: String -> [(String, String)]
lines s =
  let worker :: String -> [(Maybe Char, String)] -> [(String, String)]
      worker fromPrevNewline remainder =
        case remainder of
          [] -> [(fromPrevNewline, [])]
          ((Nothing, trailer):rest) ->
            (fromPrevNewline, trailer):(worker "" rest)
          ((Just c, _):rest) ->
            worker (fromPrevNewline ++ [c]) rest
  in worker "" $ findNewlines s

headersBody :: String -> Errorable ([String], String)
headersBody s =
  let ss = Main.lines s in
  case find (\((x,_),_) -> x == "") (zip ss $ inits ss) of
    Nothing -> tell "Message has no body?" >> return ([],s)
    Just ((_blank, body), headers) -> return (map fst headers, body) 

unfoldHeaderLines :: [String] -> [String]
unfoldHeaderLines what =
  let isLinearWhiteSpace c = c `elem` " \t"
      worker :: [String] -> (Maybe String, [String])
      worker [] = (Nothing, [])
      worker ("":others) = case worker others of
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
  
parseEmail :: String -> Errorable ([(String, String)], String)
parseEmail what =
  do (headerLines, body) <- headersBody what
     parsedHeaders <- mapM parseHeader $ unfoldHeaderLines headerLines
     return (parsedHeaders, body)
     
main :: IO ()
main =
  do print $ {-(map (fst . snd) . (take 5)) $ -}stateSequence $ [1,2,3,4,5]
     print $ toSep 'z' "abcxdefxhij"
     print $ findNewlines "foo\r\n\r\nbar\r\nbazz"
     print $ Main.lines "Hello\r\nGoodbye\r\n\r\nfoo"
     print $ runWriter $ headersBody "H1\r\nH2\r\nH3\r\n\r\nbody"
     print $ unfoldHeaderLines ["L1", " C1", " C2", "L2", "L3", "L4", " C3"]
     print $ runWriter $ parseEmail "From: foo\r\n bar\r\n\tbazz\r\nTo: Me\r\n\r\nBody"


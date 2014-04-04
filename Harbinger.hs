{-# LANGUAGE ScopedTypeVariables #-}
import Data.List

first :: (a -> b) -> (a, c) -> (b, c)
first f (x, y) = (f x, y)

type Errorable = Either [String]

instance Functor (Either a) where
  fmap f (Right x) = Right $ f x
  fmap _ (Left x) = Left x
  
{- Annoyingly, we don't quite satisfy the monad laws, because we want
to collect all of the errors rather than just the first one.  But we
might as well have the monad-like ops which do make sense. -}
joinErrors :: Either a (Either a b) -> Either a b
joinErrors (Left x) = Left x
joinErrors (Right (Left x)) = Left x
joinErrors (Right (Right x)) = Right x

instance Monad (Either a) where
  return = Right
  x >>= f = joinErrors $ fmap f x
  
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
  foldr (\elm acc ->
          case (elm,acc) of
            ((Nothing, rest),_) -> ([],rest):acc
            ((Just c, sss), ((_,rr):ss)) -> ((c:sss), rr):ss)
  [("", "")]
  (findNewlines s)

headersBody :: String -> Errorable ([String], String)
headersBody s =
  let ss = Main.lines s in
  case find (\((x,_),_) -> x == "") (zip ss $ inits ss) of
    Nothing -> Left ["Message has no body?"]
    Just ((_blank, body), headers) -> Right (map fst headers, body) 

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
    Nothing -> Left ["No : in header line " ++ hdrLine]
    Just (a, b) -> Right (a, dropWhile (flip elem " \t") b)
  
liftErrors :: [Errorable a] -> Errorable [a]
liftErrors [] = Right []
liftErrors (a:as) =
  case (a, liftErrors as) of
    (Left aErr, Left restErrs) -> Left $ aErr ++ restErrs
    (Left aErr, Right _) -> Left aErr
    (Right aRes, Right restRes) -> Right $ aRes:restRes
    (Right _, Left restErrs) -> Left restErrs

liftErrorsPair1 :: (Errorable a, b) -> Errorable (a, b)
liftErrorsPair1 (Left err1, _) = Left err1
liftErrorsPair1 (Right res1, res2) = Right (res1, res2)

parseEmail :: String -> Errorable ([(String, String)], String)
parseEmail =
  joinErrors .
  (fmap $ liftErrorsPair1 . (first $ liftErrors . map parseHeader . unfoldHeaderLines)) .
  headersBody

main :: IO ()
main =
  do print $ {-(map (fst . snd) . (take 5)) $ -}stateSequence $ [1,2,3,4,5]
     print $ toSep 'z' "abcxdefxhij"
     print $ findNewlines "foo\r\n\r\nbar\r\nbazz"
     print $ Main.lines "Hello\r\nGoodbye\r\n\r\nfoo"
     print $ headersBody "H1\r\nH2\r\nH3\r\n\r\nbody"
     print $ unfoldHeaderLines ["L1", " C1", " C2", "L2", "L3", "L4", " C3"]
     print $ parseEmail "From: foo\r\n bar\r\n\tbazz\r\nTo: Me\r\n\r\nBody"


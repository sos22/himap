module MailFilter(
  MailFilter,
  mailFilterQuery,
  parseMailFilter,
  QueryConst(..)
  ) where

import Control.Monad.Instances
import Data.Char
import Data.List
    
data DefnOp = DefnOpEq
            | DefnOpLt
            | DefnOpLe
            | DefnOpGt
            | DefnOpGe
            | DefnOpAnd
            | DefnOpOr
              deriving Show
data DefnToken = DefnTokenLBracket
               | DefnTokenRBracket
               | DefnTokenOp DefnOp
               | DefnTokenIdentifier String
               | DefnTokenString String
               | DefnTokenNumber Integer
               | DefnTokenError String
                 deriving Show
data DefnTerm = DefnString String
              | DefnNumber Integer
              | DefnIdentifier String
                deriving Show
data MailFilter = DefnAnd MailFilter MailFilter
                | DefnOr MailFilter MailFilter
                | DefnEq DefnTerm DefnTerm
                | DefnGt DefnTerm DefnTerm
                | DefnGe DefnTerm DefnTerm
                | DefnLt DefnTerm DefnTerm
                | DefnLe DefnTerm DefnTerm
                deriving Show
                  
tokenizeString' :: String -> Either String (String, String)
tokenizeString' "" = Left "EOF in string constant"
tokenizeString' ('"':cs) = Right ("", cs)
tokenizeString' ('\\':x:cs) =
  case tokenizeString' cs of
    Left err -> Left err
    Right (a, r) -> Right (x:a, r)
tokenizeString' (c:cs) =
  case tokenizeString' cs of
    Left err -> Left err
    Right (a, r) -> Right (c:a, r)
tokenizeString :: String -> (DefnToken, String)
tokenizeString x =
  case tokenizeString' x of
    Left err -> (DefnTokenError err, "")
    Right (t, rest) -> (DefnTokenString t, rest)
  
tokenizeNumber' :: String -> (Integer, Integer, String)
tokenizeNumber' "" = (1, 0, "")
tokenizeNumber' (h:t) | h `elem` "0123456789" =
  let (units, n, r) = tokenizeNumber' t
  in (units * 10, (toInteger $ digitToInt h) * units + n, r)
                       | otherwise =
    (1, 0, t)
tokenizeNumber :: String -> (DefnToken, String)
tokenizeNumber x =
  case tokenizeNumber' x of
    (1, _, r) -> (DefnTokenError "empty number?", r)
    (_, v, r) -> (DefnTokenNumber v, r)

tokenizeIdentifier :: String -> (String, String)
tokenizeIdentifier "" = ("", "")
tokenizeIdentifier r@(c:cs) | c `elem` "1234567890qwertyuiopasdfghklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_.-" =
  let (acc, rr) = tokenizeIdentifier cs
  in (c:acc, rr)
                            | otherwise =
    ("", r)
    
tokenize :: String -> [DefnToken]
tokenize "" = []
tokenize (c:cs) | c `elem` " \r\t\n\v" = tokenize cs
                | c == '(' = DefnTokenLBracket:(tokenize cs)
                | c == ')' = DefnTokenRBracket:(tokenize cs)
                | c == '&' = (DefnTokenOp DefnOpAnd):(tokenize cs)
                | c == '|' = (DefnTokenOp DefnOpOr):(tokenize cs)
                | c == '=' = (DefnTokenOp DefnOpEq):(tokenize cs)
                | c == '<' =
                  case cs of
                    ('=':ccs) -> (DefnTokenOp DefnOpLe):(tokenize ccs)
                    _ -> (DefnTokenOp DefnOpLt):(tokenize cs)
                | c == '>' =
                  case cs of
                    ('=':ccs) -> (DefnTokenOp DefnOpGe):(tokenize ccs)
                    _ -> (DefnTokenOp DefnOpGt):(tokenize cs)
                | c == '"' = 
                    let (t, r) = tokenizeString cs
                    in t:(tokenize r)
                | c `elem` "0123456789" =
                    let (t, r) = tokenizeNumber (c:cs)
                    in t:(tokenize r)
                | c `elem` "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_" =
                    let (t, r) = tokenizeIdentifier (c:cs)
                    in (DefnTokenIdentifier t):(tokenize r)
                | otherwise = [DefnTokenError $ c:cs]
                              
findClosing :: Int -> [DefnToken] -> ([DefnToken], [DefnToken])
findClosing 0 tokens' = ([], tokens')
findClosing i (DefnTokenRBracket:tokens') =
  findClosing (i - 1) tokens'
findClosing i (token:tokens') =
  let (result, leftover) = findClosing i tokens'
  in (token:result, leftover)
findClosing _ _ = ([DefnTokenError "EOF in ()"], [])

parseBoolOps :: [Either MailFilter DefnToken] -> Either String MailFilter
parseBoolOps tokens' =
  let boolOpIdx = findIndex (\x -> case x of
                                Right (DefnTokenOp DefnOpAnd) -> True
                                Right (DefnTokenOp DefnOpOr) -> True
                                _ -> False) tokens'
  in case boolOpIdx of
    Just 0 -> Left "& and | cannot start filter"
    Just idx ->
      do l <- parseBoolOps $ take idx tokens'
         r <- parseBoolOps $ drop (idx + 1) tokens'
         return $ (case tokens' !! idx of
                      Right (DefnTokenOp DefnOpAnd) -> DefnAnd
                      Right (DefnTokenOp DefnOpOr) -> DefnOr
                      _ -> error "wibble") l r
    Nothing ->
      let relOpIdx = findIndex (\x -> case x of
                                   Right (DefnTokenOp _) -> True
                                   _ -> False) tokens'
      in case relOpIdx of
        Just 0 -> Left "cannot start filter with <, >, =, <=, >="
        Just idx ->
          do l <- parseNonOps $ take idx tokens'
             r <- parseNonOps $ drop (idx + 1) tokens'
             return $ (case tokens' !! idx of
                            Right (DefnTokenOp DefnOpEq) -> DefnEq
                            Right (DefnTokenOp DefnOpLt) -> DefnLt
                            Right (DefnTokenOp DefnOpGt) -> DefnGt
                            Right (DefnTokenOp DefnOpLe) -> DefnLe
                            Right (DefnTokenOp DefnOpGe) -> DefnGe
                            _ -> error "wobble") l r
        Nothing ->
          case tokens' of
            [Left r] -> return r
            _ -> Left $ "Cannot parse " ++ (show tokens')


parseNonOps tokens' =
  case tokens' of
    [Right (DefnTokenIdentifier n)] -> Right $ DefnIdentifier n
    [Right (DefnTokenString s)] -> Right $ DefnString s
    [Right (DefnTokenNumber n)] -> Right $ DefnNumber n
    (Right (DefnTokenError e)):_ -> Left e
    _ -> Left "cannot parse term"

parseBracketed :: [DefnToken] -> [Either MailFilter DefnToken]
parseBracketed [] = []
parseBracketed (DefnTokenLBracket:tokens') =
  let (expr1, leftover) = findClosing 1 tokens'
  in (case parseBoolOps $ map Right expr1 of
         Left err -> Right $ DefnTokenError err
         Right x -> Left x):(parseBracketed leftover)
parseBracketed (token:tokens') =
  (Right token):(parseBracketed tokens')

mboxDefnParser :: [DefnToken] -> Maybe MailFilter
mboxDefnParser tokens =
  case parseBoolOps $ parseBracketed tokens of
    Left _ -> Nothing
    Right x -> Just x

parseMailFilter :: String -> Maybe MailFilter
parseMailFilter = mboxDefnParser . tokenize

combine :: Ord a => [a] -> [a] -> [a]
combine a b = merge (sort a) (sort b)
  where merge a [] = a
        merge [] b = b
        merge a@(aa:as) b@(bb:bs) =
          if aa < bb
          then aa:(merge as b)
          else if aa == bb
               then aa:(merge as bs)
               else bb:(merge a bs)

collectAttributes' :: DefnTerm -> [String]
collectAttributes' (DefnIdentifier x) = [x]
collectAttributes' (DefnString _) = []
collectAttributes' (DefnNumber _) = []

collectAttributes :: MailFilter -> [String]
collectAttributes (DefnAnd a b) =
  combine (collectAttributes a) (collectAttributes b)
collectAttributes (DefnOr a b) =
  combine (collectAttributes a) (collectAttributes b)
collectAttributes (DefnEq a b) =
  combine (collectAttributes' a) (collectAttributes' b)
collectAttributes (DefnGt a b) =
  combine (collectAttributes' a) (collectAttributes' b)
collectAttributes (DefnLt a b) =
  combine (collectAttributes' a) (collectAttributes' b)
collectAttributes (DefnGe a b) =
  combine (collectAttributes' a) (collectAttributes' b)
collectAttributes (DefnLe a b) =
  combine (collectAttributes' a) (collectAttributes' b)

data QueryConst = ConstString String
                | ConstNumber Integer
                  deriving (Show, Ord, Eq)
collectConsts' :: DefnTerm -> [QueryConst]
collectConsts' (DefnIdentifier _) = []
collectConsts' (DefnString x) = [ConstString x]
collectConsts' (DefnNumber x) = [ConstNumber x]
collectConsts :: MailFilter -> [QueryConst]
collectConsts (DefnAnd a b) =
  combine (collectConsts a) (collectConsts b)
collectConsts (DefnOr a b) =
  combine (collectConsts a) (collectConsts b)
collectConsts (DefnEq a b) =
  combine (collectConsts' a) (collectConsts' b)
collectConsts (DefnGt a b) =
  combine (collectConsts' a) (collectConsts' b)
collectConsts (DefnLt a b) =
  combine (collectConsts' a) (collectConsts' b)
collectConsts (DefnGe a b) =
  combine (collectConsts' a) (collectConsts' b)
collectConsts (DefnLe a b) =
  combine (collectConsts' a) (collectConsts' b)

queryToFilter' _ c (DefnString s) =
  case lookup (ConstString s) c of
    Nothing -> error "Lost a string"
    Just x -> "@" ++ x
queryToFilter' _ c (DefnNumber n) =
  case lookup (ConstNumber n) c of
    Nothing -> error "Lost a number"
    Just x -> "@" ++ x
queryToFilter' attribs _ (DefnIdentifier l) =
  case lookup l attribs of
    Nothing -> error "lost an attribute"
    Just x -> x ++ ".Value"
    
queryToFilter l c (DefnAnd a b) = "(" ++ (queryToFilter l c a)  ++ " AND " ++ (queryToFilter l c b) ++ ")"
queryToFilter l c (DefnOr a b)  = "(" ++ (queryToFilter l c a)  ++ " OR "  ++ (queryToFilter l c b) ++ ")"
queryToFilter l c (DefnEq a b)  = "(" ++ (queryToFilter' l c a) ++ " = "   ++ (queryToFilter' l c b) ++ ")"
queryToFilter l c (DefnLt a b)  = "(" ++ (queryToFilter' l c a) ++ " < "   ++ (queryToFilter' l c b) ++ ")"
queryToFilter l c (DefnLe a b)  = "(" ++ (queryToFilter' l c a) ++ " <= "  ++ (queryToFilter' l c b) ++ ")"
queryToFilter l c (DefnGt a b)  = "(" ++ (queryToFilter' l c a) ++ " > "   ++ (queryToFilter' l c b) ++ ")"
queryToFilter l c (DefnGe a b)  = "(" ++ (queryToFilter' l c a) ++ " >= "  ++ (queryToFilter' l c b) ++ ")"

mailFilterQuery :: MailFilter -> (String, [(String, String)], [(QueryConst, String)])
mailFilterQuery mf =
  let attribs = collectAttributes mf
      consts = collectConsts mf
      attribs' = case attribs of
        [] -> ["rfc822.Message-Id"]
        _ -> attribs
      namedAttribs = zip attribs' (map (\x -> 'z':(show x)) [0..]) 
      namedConsts = zip consts (map (\x -> 'z':(show x)) [(length namedAttribs)..])
      idTable = snd $ head namedAttribs
      joinPart = concat [" JOIN MessageAttrs AS " ++ name ++ " ON " ++ idTable ++ ".MessageId = " ++ name ++ ".MessageId WHERE " ++ name ++ ".AttributeId = @" ++ name | (_, name) <- tail namedAttribs]
      filterPart = queryToFilter namedAttribs namedConsts mf
  in ("SELECT " ++ idTable ++ ".MessageId FROM MessageAttrs AS " ++ idTable ++ " WHERE " ++ idTable ++ ".AttributeId = @" ++ idTable ++ " " ++ joinPart ++ " AND " ++ filterPart,
      namedAttribs,
      namedConsts)
     
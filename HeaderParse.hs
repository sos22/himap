{- Thing to get headers into a canonical format for the database index. -}
module HeaderParse(parseHeader) where

import Control.Monad
import Data.Char
import Data.List
import qualified Data.Text as DT
import qualified Database.SQLite3 as DS
import Debug.Trace

import Email

{- Less a parser and more a canonicaliser.  Maps from strings (usually
   header values) to lists of things under which the message should be indexed. -}
data Parser a = Parser { run_parser :: Bool -> String -> Either String (a, Bool, String) }

instance Monad Parser where
  return x = Parser $ \c r -> Right (x, c, r)
  fstM >>= sndF = Parser $ \c initialStr ->
    case run_parser fstM c initialStr of
      Left err -> Left err
      Right (res1, c', midStr) -> run_parser (sndF res1) c' midStr
  
parseAlternatives :: [Parser a] -> Parser a
parseAlternatives [] = Parser $ \_ _ -> Left "ran out of alternatives"
parseAlternatives (a:as) = Parser $ \c str ->
  case run_parser a c str of
    Left _ -> run_parser (parseAlternatives as) c str
    Right (res, c', leftover) -> Right (res, c', leftover)

parseNonEmpty :: String -> Parser a -> Parser a
parseNonEmpty msg parser =
  Parser $ \_ initStr ->
  case run_parser parser False initStr of
    Left err -> Left err
    Right (_, False, "") -> Left "parseNonEmpty empty string at end of file"
    Right (_, False, leftover) -> Left $ msg ++ " failed to consume any input at " ++ (show initStr) ++ ", leftover " ++ (show leftover)
    Right (res, True, leftover) -> Right (res, True, leftover)
    
parseMany1 :: Parser a -> Parser [a]
parseMany1 p =
  do one <- parseNonEmpty "many1" p
     rest <- parseAlternatives [parseMany1 p, return []]
     return $ one:rest
parseMany :: Parser a -> Parser [a]
parseMany p = parseAlternatives [parseMany1 p, return []]
parseMany1Sep :: Parser a -> Parser b -> Parser [a]
parseMany1Sep thing seperator =
  parseAlternatives [do one <- parseNonEmpty "many1sep" (do x <- thing
                                                            _ <- seperator
                                                            return x)
                        rest <- parseMany1Sep thing seperator
                        return $ one:rest,
                     do one <- parseNonEmpty "many1sep singleton" thing
                        return [one]]
parseOptional :: Parser a -> Parser (Maybe a)
parseOptional what = parseAlternatives [ liftM Just what,
                                         return Nothing ]
parseChar :: Char -> Parser Char
parseChar c = Parser $ \_ x -> case x of
  "" -> Left $ "EOF looking for " ++ [c]
  (cc:ccs) | cc == c -> Right (c, True, ccs)
           | otherwise -> Left $ "Wanted " ++ [c] ++ ", got "++ x
parseCharRange :: Int -> Int -> Parser Char
parseCharRange start end = Parser $ \_ x ->
  case x of
    [] -> Left $ "EOF looking for >= " ++ (show start) ++ " <= " ++ (show end)
    xx:xs ->
      if ord xx >= start && ord xx <= end
      then Right (xx, True, xs)
      else Left $ "Wanted >= " ++ (show start) ++ " <= " ++ (show end) ++ ", got " ++ x

runParser :: Show a => Parser a -> String -> Either String a
runParser p what =
  case run_parser p False what of
    Right (x, _, "") -> trace ("Parsing " ++ what ++ " gave " ++ (show x)) $
                        Right x
    Right (_, _, leftovers) -> Left $ "Junk at end of field? " ++ (show leftovers)
    Left err -> Left $ "Error " ++ (show err) ++ " parsing " ++ (show what)

-- This is a fairly direct transcript of the grammar from RFC2822,
-- because it's easier to do that than to sit and thinnk about what it
-- all means.
parseMsgId :: Parser DS.SQLData
parseMsgId = do _ <- parseOptional $ parseCFWS
                _ <- parseChar '<'
                l <- parseIdLeft
                _ <- parseChar '@'
                r <- parseIdRight
                _ <- parseChar '>'
                _ <- parseOptional $ parseCFWS
                return $ DS.SQLText $ DT.pack $ l ++ "@" ++ r
parseIdLeft :: Parser String
parseIdLeft = parseAlternatives [parseDotAtomText, parseNoFoldQuote, parseObsIdLeft]
parseIdRight :: Parser String
parseIdRight = parseAlternatives [parseDotAtomText, parseNoFoldLiteral, parseObsIdRight]
parseNoFoldQuote :: Parser String
parseNoFoldQuote = do _ <- parseChar '"'
                      _ <- parseOptional $ parseChar '\''
                      r <- liftM concat $ parseMany (parseAlternatives [liftM singleton parseQtext, parseQuotedPair])
                      _ <- parseOptional $ parseChar '\''
                      _ <- parseChar '"'
                      return r
parseNoFoldLiteral :: Parser String
parseNoFoldLiteral = do _ <- parseChar '['
                        r <- liftM concat $ parseMany (parseAlternatives [liftM singleton parseDtext, parseQuotedPair])
                        _ <- parseChar ']'
                        return r
parseMailboxList :: Parser [String]
parseMailboxList = parseAlternatives [liftM concat $ parseMany1Sep parseMailbox (parseChar ','),
                                      parseObsMboxList]
parseAddressList :: Parser [String]
parseAddressList = parseAlternatives [liftM concat $ parseMany1Sep parseAddress (parseChar ','),
                                      parseObsAddrList]
parseUnstructured :: Parser [String]
parseUnstructured = do r <- parseMany $ do a <- parseOptional parseFWS
                                           b <- parseUtext
                                           return $ case a of
                                             Nothing -> b
                                             Just _ -> ' ':b
                       _ <- parseOptional parseFWS
                       return [concat r]
parseUtext :: Parser String
parseUtext = parseAlternatives [liftM (\x -> [x]) parseNoWsCtl,
                                liftM (\x -> [x]) $ parseCharRange 33 126,
                                parseObsUtext]
parsePhrase :: Parser String
parsePhrase = parseAlternatives [liftM (intercalate " ") $ parseMany1 parseWord, parseObsPhrase]
parseWord :: Parser String
parseWord = parseAlternatives [parseAtom, parseQuotedString]
parseAddress :: Parser [String]
parseAddress = parseAlternatives [parseMailbox, parseGroup]
parseMailbox :: Parser [String]
parseMailbox = parseAlternatives [parseNameAddr, liftM (\x -> [x]) parseAddrSpec]
parseNameAddr :: Parser [String]
parseNameAddr = do dn <- parseOptional parseDisplayName
                   aa <- parseAngleAddr
                   return $ case dn of
                     Just dn' -> [dn',aa]
                     Nothing -> [aa]
parseAngleAddr :: Parser String
parseAngleAddr = parseAlternatives [do _ <- parseOptional parseCFWS
                                       as <- do _ <- parseChar '<'
                                                r <- parseAddrSpec
                                                _ <- parseChar '>'
                                                return r
                                       _ <- parseOptional parseCFWS
                                       return as,
                                    parseObsAngleAddr]
parseGroup :: Parser [String]
parseGroup = do dn <- parseDisplayName
                _ <- parseChar ':'
                ml <- parseOptional $ parseAlternatives [parseMailboxList, parseCFWS >> return []]
                _ <- parseChar ';'
                _ <- parseOptional parseCFWS
                return $ case ml of
                  Just x -> dn:x
                  Nothing -> [dn]
parseDisplayName :: Parser String
parseDisplayName = parsePhrase
parseAddrSpec :: Parser String
parseAddrSpec = do l <- parseLocalPart
                   _ <- parseChar '@'
                   d <- parseDomain
                   return $ l ++ "@" ++ d
parseLocalPart :: Parser String
parseLocalPart = parseAlternatives [parseDotAtom, parseQuotedString, parseObsLocalPart]
parseDomain :: Parser String
parseDomain = parseAlternatives [parseDotAtom, parseDomainLiteral, parseObsDomain]
parseDomainLiteral :: Parser String
parseDomainLiteral = do _ <- parseOptional parseCFWS
                        _ <- parseChar '['
                        r <- parseMany $ do a <- parseOptional parseFWS
                                            r <- parseDcontent
                                            return $ case a of
                                              Nothing -> r
                                              Just _ -> ' ':r
                        _ <- parseOptional parseFWS
                        _ <- parseChar ']'
                        _ <- parseOptional parseCFWS
                        return $ concat r
parseDcontent :: Parser String
parseDcontent = parseAlternatives [liftM singleton parseDtext, parseQuotedPair]
parseDtext :: Parser Char
parseDtext = parseAlternatives [parseNoWsCtl, parseCharRange 33 90, parseCharRange 94 126]
parseNoWsCtl :: Parser Char
parseNoWsCtl = parseAlternatives [parseCharRange 1 8,
                                  parseCharRange 11 12,
                                  parseCharRange 14 31,
                                  parseCharRange 127 127]
singleton :: a -> [a]
singleton x = [x]
parseText :: Parser String
parseText = parseAlternatives [liftM singleton $ parseCharRange 1 9,
                               liftM singleton $ parseCharRange 11 12,
                               liftM singleton $ parseCharRange 14 127,
                               parseObsText]
parseQuotedPair :: Parser String
parseQuotedPair = parseAlternatives [(parseChar '\\') >> parseText,
                                     liftM singleton parseObsQp]
parseFWS :: Parser ()
parseFWS = parseAlternatives [parseMany1 parseWSP >> return (), parseObsFWS] >> return ()
parseCFWS :: Parser ()
parseCFWS = do _ <- parseMany (parseOptional parseFWS >> parseComment)
               parseAlternatives [(parseOptional parseFWS) >> parseComment,
                                  parseFWS]
               return ()
parseComment :: Parser ()
parseComment = do _ <- parseChar '('
                  _ <- parseMany (parseFWS >> parseCContent)
                  parseFWS
                  _ <- parseChar ')'
                  return ()
parseCContent :: Parser ()
parseCContent = parseAlternatives [parseCText, parseQuotedPair >> return (), parseComment] >> return ()
parseCText :: Parser ()
parseCText = parseAlternatives [parseNoWsCtl,
                                parseCharRange 33 39,
                                parseCharRange 41 91,
                                parseCharRange 93 126] >> return ()
parseWSP :: Parser ()
parseWSP = (parseAlternatives $ map parseChar " \r\t\n\v") >> return ()
parseDotAtom :: Parser String
parseDotAtom = do _ <- parseOptional parseCFWS
                  r <- parseDotAtomText
                  _ <- parseOptional parseCFWS
                  return r
parseDotAtomText :: Parser String
parseDotAtomText = liftM (intercalate ".") $ parseMany1Sep (parseMany1 parseAText) (parseChar '.')
parseAText :: Parser Char
parseAText = parseAlternatives $ map parseChar "1234567890qwertyuiopasdfghklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM!#$%&\'*+-/=?^_`{|}~"
parseAtom :: Parser String
parseAtom = do _ <- parseOptional parseCFWS
               r <- parseMany1 parseAText
               _ <- parseOptional parseCFWS
               return r
parseQuotedString :: Parser String
parseQuotedString = do _ <- parseOptional parseCFWS
                       _ <- parseChar '"'
                       r <- parseMany $ do a <- parseOptional parseFWS
                                           b <- parseQcontent
                                           return $ case a of
                                             Nothing -> b
                                             Just _ -> ' ':b
                       _ <- parseChar '"'
                       _ <- parseOptional parseCFWS
                       return $ concat r
parseQcontent :: Parser String
parseQcontent = parseAlternatives [liftM singleton parseQtext, parseQuotedPair]
parseQtext :: Parser Char
parseQtext = parseAlternatives [parseNoWsCtl,
                                parseCharRange 33 33,
                                parseCharRange 35 91,
                                parseCharRange 93 126]
parseObsIdLeft :: Parser String
parseObsIdLeft = parseLocalPart
parseObsIdRight :: Parser String
parseObsIdRight = parseDomain
parseObsMboxList :: Parser [String]
parseObsMboxList =
  do r <- liftM concat $ parseMany1 $ do rr <- parseOptional parseMailbox
                                         _ <- parseOptional parseCFWS
                                         _ <- parseChar ','
                                         _ <- parseOptional parseCFWS
                                         return $ maybe [] id rr
     l <- parseOptional parseMailbox
     return $ case l of
       Nothing -> r
       Just ls -> ls ++ r
parseObsAddrList :: Parser [String]
parseObsAddrList = do r <- parseMany1 $ do rr <- parseOptional parseAddress
                                           _ <- parseOptional parseCFWS
                                           _ <- parseChar ','
                                           _ <- parseOptional parseCFWS
                                           return $ maybe [] id rr
                      l <- parseOptional parseAddress
                      return $ (concat r) ++ (maybe [] id l)
parseObsAngleAddr :: Parser String
parseObsAngleAddr = do _ <- parseOptional parseCFWS
                       _ <- parseChar '<'
                       _ <- parseOptional parseObsRoute
                       a <- parseAddrSpec
                       _ <- parseChar '>'
                       _ <- parseOptional parseCFWS
                       return a
parseObsRoute :: Parser String
parseObsRoute = do _ <- parseOptional parseCFWS
                   dl <- parseObsDomainList
                   _ <- parseChar ':'
                   _ <- parseOptional parseCFWS
                   return dl
parseObsDomainList :: Parser String
parseObsDomainList = do _ <- parseChar '@'
                        d <- parseDomain
                        ds <- parseMany $ do _ <- parseMany $ parseAlternatives [parseCFWS, parseChar ',' >> return ()]
                                             _ <- parseOptional parseCFWS
                                             _ <- parseChar '@'
                                             parseDomain
                        return $ '@':(intercalate "@" (d:ds))
parseObsDomain :: Parser String
parseObsDomain = liftM (intercalate ".") $ parseMany1Sep parseAtom (parseChar '.')
parseObsLocalPart :: Parser String
parseObsLocalPart = liftM (intercalate ".") $ parseMany1Sep parseWord (parseChar '.')
parseObsUtext :: Parser String
parseObsUtext = parseObsText
parseObsText :: Parser String
parseObsText = do _ <- parseMany $ parseChar '\r'
                  _ <- parseMany $ parseChar '\n'
                  liftM concat $ parseMany $ do rr <- parseObsChar
                                                a <- parseMany $ parseChar '\r'
                                                b <- parseMany $ parseChar '\n'
                                                return $ case (a, b) of
                                                  ("", "") -> [rr]
                                                  _ -> rr:" "
parseObsChar :: Parser Char
parseObsChar = parseAlternatives [ parseCharRange 0 9,
                                   parseCharRange 11 12,
                                   parseCharRange 14 127]
parseObsPhrase :: Parser String
parseObsPhrase = do w <- parseWord
                    r <- parseMany $ parseAlternatives [parseWord,
                                                        liftM singleton $ parseChar '.',
                                                        parseCFWS >> return ""]
                    return $ intercalate " " $ w:r
parseObsQp :: Parser Char
parseObsQp = (parseChar '\\') >> parseCharRange 0 127
parseObsFWS :: Parser ()
parseObsFWS = (parseMany1 parseWSP) >> (parseMany (parseChar '\r' >> parseChar '\n' >> parseMany1 parseWSP)) >> return ()

stringField :: Parser [String] -> Parser [DS.SQLData]
stringField = liftM (map $ DS.SQLText . DT.pack)

-- mapping from RFC822 header name to (attribute name, attribute
-- parser) pairs.
parsers :: [(String, (String, Parser [DS.SQLData]))]
parsers = [("in-reply-to", ("rfc822.In-Reply-To", parseMany1 parseMsgId)),
           ("references", ("rfc822.References", parseMany1 parseMsgId)),
           ("from", ("rfc822.From", stringField parseMailboxList)),
           ("sender", ("rfc822.Sender", stringField parseAddressList)),
           ("reply-to", ("rfc822.Reply-To", stringField parseAddressList)),
           ("to", ("rfc822.To", stringField parseAddressList)),
           ("cc", ("rfc822.Cc", stringField parseAddressList)),
           ("bcc", ("rfc822.Bcc", stringField parseAddressList)),
           ("subject", ("rfc822.Subject", stringField parseUnstructured)),
           ("comments", ("rfc822.Comments", stringField parseUnstructured)),
           ("keywords", ("rfc822.Keywords", stringField $ parseMany1Sep parsePhrase (parseChar ','))),
           ("date", ("rfc822.Date", stringField $ parseUnstructured))]

parseHeader :: Header -> Either String [(String, DS.SQLData)]
parseHeader (Header name value) =
  case lookup (map toLower name) parsers of
    Just (fieldname, parser) ->
      case runParser parser value of
        Left err -> Left $ "cannot parse header " ++ name ++ ", value " ++ (show value) ++ ": " ++ err
        Right val -> Right $ [(fieldname, v) | v <- val]
    Nothing -> Right [] {- Not an indexed field -}
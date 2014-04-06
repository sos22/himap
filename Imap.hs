import Network
import qualified Data.ByteString as BS
import System.IO
import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Char
import Debug.Trace

instance Functor (Either a) where
  fmap f (Right x) = Right $ f x
  fmap _ (Left x) = Left x
  
second :: (a -> b) -> (c, a) -> (c, b)
second f (x, y) = (x, f y)

ignore :: Monad m => m a -> m ()
ignore = flip (>>) $ return ()

stringToByteString :: String -> BS.ByteString
stringToByteString = BS.pack . map (fromInteger.toInteger.ord)
byteStringToString :: BS.ByteString -> String
byteStringToString = map (chr.fromInteger.toInteger) . BS.unpack
bsEOL :: BS.ByteString
bsEOL = stringToByteString "\r\n"

data ImapServerState = ImapServerState { iss_handle :: Handle, 
                                         iss_outgoing_response :: [BS.ByteString],
                                         iss_incoming_buffer :: BS.ByteString
                                         }

data ImapServer a = ImapServer { run_is :: ImapServerState -> IO (Maybe (ImapServerState, a)) }
instance Monad ImapServer where
  return x = ImapServer $ \state -> return $ Just (state, x)
  first >>= secondF = ImapServer $ \state ->
    do firstRes <- run_is first state
       case firstRes of
         Nothing -> return Nothing
         Just (newState, firstRes') ->
           run_is (secondF firstRes') newState
           
runImapServer :: Handle -> ImapServer () -> IO ()
runImapServer hndle is =
  ignore $ run_is is $ ImapServerState { iss_handle = hndle, 
                                         iss_outgoing_response = [],
                                         iss_incoming_buffer = BS.empty }

queueResponse :: String -> ImapServer ()
queueResponse what = ImapServer $ \isState ->
  return $ Just (isState {iss_outgoing_response = (stringToByteString what):
                                                  (iss_outgoing_response isState)},
                 ())

finishResponse :: ImapServer ()
finishResponse = ImapServer $ \isState ->
  let worker [] = return ()
      worker (x:xs) = worker xs >> BS.hPut (iss_handle isState) x
  in do worker (iss_outgoing_response isState)
        hFlush (iss_handle isState)
        return $ Just (isState {iss_outgoing_response = []}, ())

data ResponseTag = ResponseUntagged
                 | ResponseTagged String
                   deriving Show
data ResponseState = ResponseStateOk
                   | ResponseStateNo
                   | ResponseStateBad
                   | ResponseStateNone
                     deriving Show
data ResponseAttribute = ResponseAttribute
                         deriving Show
attrToString :: ResponseAttribute -> String
attrToString = error "No attributes yet"

data ImapVerb = ImapNoop
              | ImapCapability
              | ImapBadCommand
              deriving Show
data ImapArg = ImapArg String
             deriving Show
data ImapCommand = ImapCommand { imc_tag :: ResponseTag,
                                 imc_verb :: ImapVerb,
                                 imc_args :: [ImapArg] }
                   deriving Show
     
sendResponse :: ResponseTag -> ResponseState -> [ResponseAttribute] -> String -> ImapServer ()
sendResponse tag state attrs resp =
  do case tag of
       ResponseUntagged -> queueResponse "*"
       ResponseTagged tag' -> queueResponse tag'
     queueResponse " "
     case state of
       ResponseStateOk -> queueResponse "OK "
       ResponseStateNo -> queueResponse "NO "
       ResponseStateBad -> queueResponse "BAD "
       ResponseStateNone -> return ()
     case map attrToString attrs of
       [] -> return ()
       attrs' -> let w [] = queueResponse "]"
                     w [x] = queueResponse x >> w []
                     w (x:xs) = queueResponse x >> queueResponse " " >> w xs
                 in queueResponse " [" >> w attrs'
     queueResponse resp
     queueResponse "\r\n"
     finishResponse

sendResponseOk :: ResponseTag -> [ResponseAttribute] -> String -> ImapServer ()
sendResponseOk t = sendResponse t ResponseStateOk

sendResponseBad :: ResponseTag -> [ResponseAttribute] -> String -> ImapServer ()
sendResponseBad t = sendResponse t ResponseStateBad

data Parser tokens res = Parser { run_parser :: [tokens] -> Either String ([tokens], res) }

instance Monad (Parser tokens) where
  return x = Parser $ \tokens -> Right (tokens, x)
  first >>= secondF = Parser $ \tokens ->
    case run_parser first tokens of
      Left err -> Left err
      Right (leftover, res1) ->
        run_parser (secondF res1) leftover
        
runParser :: Parser tokens a -> [tokens] -> Either String a
runParser parser string = fmap snd $ run_parser parser string

parserTakeWhile :: (a -> Bool) -> Parser a [a]
parserTakeWhile predicate =
  Parser pp where pp = \tokens ->
                    case tokens of
                      [] -> Right ([], [])
                      (tok:others) | predicate tok ->
                        fmap (second $ (:) tok) $ pp others
                      leftover -> Right (leftover, [])

parserSplitOn :: (a -> Bool) -> Parser a [[a]]
parserSplitOn predicate =
  Parser (\tokens ->
           case foldr
                (\tok (line, otherLines) -> if predicate tok
                                            then ([], line:otherLines)
                                            else (tok:line, otherLines))
                ([], [])
                tokens of
             (l, res) -> Right ([], l:res))
  
parseThis :: Eq a => a -> Parser a ()
parseThis what = Parser $ \tokens -> case tokens of
  [] -> Left "Unexpected EOF"
  (x:xs) | what == x -> Right (xs, ())
         | otherwise -> Left "parseThis failed"
                        
infixr 1 <.>
(<.>) :: Parser Char a -> Parser Char b -> Parser Char (a, b)
(<.>) parser1 parser2 =
  do r1 <- parser1
     r2 <- parser2
     return (r1, r2)

orEof :: Parser c a -> Parser c (Maybe a)
orEof parser = Parser $ \tokens -> case tokens of
  [] -> Right ([], Nothing)
  _ -> fmap (second Just) $ run_parser parser tokens

parseTag :: Parser Char ResponseTag
parseTag = do pp <- parserTakeWhile $ (/=) ' '
              parseThis ' '
              return $ if pp == "*"
                       then ResponseUntagged
                       else ResponseTagged pp

parseVerb :: Parser Char ImapVerb
parseVerb = liftM worker $ parserTakeWhile $ (/=) ' '
            where worker "NOOP" = ImapNoop
                  worker "CAPABILITY" = ImapCapability
                  worker _ = ImapBadCommand

parseArgs :: Parser Char [ImapArg]
parseArgs = (parseThis ' ') >> (liftM (map ImapArg) $ parserSplitOn $ (==) ' ')

parseIncomingCommand :: String -> Either String ImapCommand
parseIncomingCommand s =
  case runParser (parseTag <.> parseVerb <.> (orEof parseArgs)) s of
    Left err -> Left err
    Right (tag, (verb, args)) -> Right $ ImapCommand tag verb (maybe [] id args)

processCommand :: (Either String ImapCommand) -> ImapServer ()
processCommand (Left err) = sendResponseBad ResponseUntagged [] $ "Cannot parse command: " ++ err
processCommand (Right cmd) =
  trace ("Run command " ++ (show cmd)) $
  case imc_verb cmd of
    ImapCapability -> do sendResponse ResponseUntagged ResponseStateNone [] "CAPABILITY IMAP4rev1"
                         sendResponseOk (imc_tag cmd) [] "Done capability"
    ImapNoop -> sendResponseOk (imc_tag cmd) [] "Done noop"
    ImapBadCommand -> sendResponseBad (imc_tag cmd) [] "Bad command"

readCommand :: ImapServer (Either String ImapCommand)
readCommand =
  ImapServer worker
  where worker isState =
          case BS.findSubstring bsEOL $ iss_incoming_buffer isState of
            Just idx -> let (thisCommand, newBuf) = BS.splitAt idx $ iss_incoming_buffer isState
                        in return $ Just $ (isState { iss_incoming_buffer = BS.drop 2 newBuf },
                                            parseIncomingCommand $ byteStringToString thisCommand)
            Nothing ->
              do nextFrag <- BS.hGetSome (iss_handle isState) 4096
                 if BS.length nextFrag == 0
                   then return Nothing
                   else worker (isState { iss_incoming_buffer = BS.append (iss_incoming_buffer isState) nextFrag })

processClient :: Handle -> IO ()
processClient clientHandle =
  runImapServer clientHandle $ do sendResponseOk ResponseUntagged [] "Hello"
                                  forever (readCommand >>= processCommand)
     
main :: IO ()
main =
  withSocketsDo $
  do listenSock <- listenOn $ PortNumber 5000
     forever $ do (clientHandle, clientHost, clientPort) <- accept listenSock
                  print $ "Accepted from " ++ clientHost ++ ", " ++ (show clientPort)
                  ignore $ forkIO $  finally (processClient clientHandle) (hClose clientHandle)

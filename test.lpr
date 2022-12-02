program test;

uses Classes, SysUtils, rstomp;

type

  { TEvent }

 TEvent = class
    class procedure RecvConnectedHandler(Frame: TRStompFrame);
    class procedure RecvMessageHandler(Frame: TRStompFrame);
    class procedure RecvReceiptHandler(Frame: TRStompFrame);
    class procedure RecvErrorHandler(Frame: TRStompFrame);
  end;

const
  PORT = 61613;

var
  stomp: TRStomp;
  i: Integer;

{ TEvent }

class procedure TEvent.RecvConnectedHandler(Frame: TRStompFrame);
begin
  WriteLn();
  WriteLn('<<< ', frame.GetPacket());
  WriteLn();
end;

class procedure TEvent.RecvMessageHandler(Frame: TRStompFrame);
begin
	RecvConnectedHandler(Frame);
end;

class procedure TEvent.RecvReceiptHandler(Frame: TRStompFrame);
begin
  RecvConnectedHandler(Frame);
end;

class procedure TEvent.RecvErrorHandler(Frame: TRStompFrame);
begin
  RecvConnectedHandler(Frame);
end;

begin
	stomp:= TRStomp.Create('192.168.28.2', PORT);
  stomp.Login:= 'dev';
  stomp.Passcode:= 'dev123';
  stomp.Vhost:= '/';
  stomp.OnRecvConnected:= @TEvent.RecvConnectedHandler;
  stomp.OnRecvMessage:= @TEvent.RecvMessageHandler;
  stomp.OnRecvReceipt:= @TEvent.RecvReceiptHandler;
  stomp.OnRecvError:= @TEvent.RecvErrorHandler;
  if stomp.Connect() then
  	WriteLn('Connection to server established.')
  else
    WriteLn('Connecting error.');

  WriteLn('Subscribe the /topic/test.');
  stomp.Receipt:= 'msg-0000';
  stomp.Subscribe('sub-01', '/topic/test');

  // send message to /topic/test.
  WriteLn('Send first message to /topic/test.');
  stomp.Send('/topic/test', 'Hello world!');

  // send second message to /topic/test.
  WriteLn('Send second message to /topic/test.');
  stomp.Send('/topic/test', 'This is the second message from client.');

  for i := 0 to 4 do
  begin
  	WriteLn('Send the other 9 messages to /topic/test.');
    stomp.Send('/topic/test', 'This is the ' + IntToStr(i+1) + ' of other message from client.');
  end;

  // unsubscribe topic.
  //WriteLn('Unsubscribe topic.');
  //stomp.Unsubscribe('sub-01');

  // send receipt frame.
  WriteLn('Send frame with receipt header.');
  stomp.Receipt:= 'msg-00001';
  stomp.Send('/topic/test', 'This message sent with receipt header.');

  // begin transaction.
  WriteLn('Begin transaction testing. Send BEGIN frame.');
  stomp.Receipt:= 'msg-00002';
  stomp.BeginTx('Tx-001');

  // commit trasaction.
  WriteLn('Commit transaction testing. Send COMMIT frame');
  stomp.Receipt:= 'msg-00003';
  stomp.CommitTx('Tx-001');

  WriteLn('Ack with receipt testing.');
  stomp.Receipt:= 'msg-00004';
  stomp.Ack('01');

  WriteLn('NAck with receipt testing.');
  stomp.Receipt:= 'msg-00005';
  stomp.NAck('01');

  Readln;
end.


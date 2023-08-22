{*******************************************************************************

	STOMP connector for FreePascal.

  Author: Riza Anshari
  Github: github.com/itsmeriza
  Twitter: @RizaAnshari
  Email: rizast@gmail.com
  License: MIT License

  Copyright (c) 2022 - Riza Anshari.

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.
  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.

*******************************************************************************}


unit rstomp;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, windows, SysUtils, IdTCPClient, IdThreadComponent, StrUtils,
  syncobjs;

type
  TRStompCommand = (scSend, scSubscribe, scUnsubscribe, scBegin, scCommit, scAbort,
  	scAck, scNAck, scDisconnect, scConnect, scStomp, scConnected, scMessage, scReceipt,
    scError);

  TRStompAckType = (atAuto, atClient, atIndividual);

  { TRStompHeader }

  TRStompHeader = class
  private
    fName: string;
    fValue: string;
  public
    constructor Create(Header: string); overload;
    function GetString(): string;
  published
    property Name: string read fName write fName;
    property Value: string read fValue write fValue;
  end;

  { TRStompFrame }

  TRStompFrame = class
  private
    fReceipt: string;
    fRawFrame: string;
  protected
    fBody: string;
    fCommand: TRStompCommand;
    fHeaders: TList;
    fContentType: string;

    procedure DoAddHeader(Name, Value: string); overload;
    procedure DoAddHeaders(); virtual;
  public
    constructor Create(Command: TRStompCommand);
    destructor Destroy; override;

    function GetPacket(): string;
    function GetHeaderString: string;
    function GetBody(): string;
    function GetRawFrame(): string;
    function GetHeader(HeaderName: string): TRStompHeader;
    function GetAckId(): string;
		procedure SetRawFrame(Value: string);
    procedure AddHeader(Name, Value: string); overload;
    procedure AddHeader(Header: string); overload;
 	published
    property Receipt: string read fReceipt write fReceipt;
  	property Command: TRStompCommand read fCommand write fCommand;
    property Headers: TList read fHeaders write fHeaders;
    property Body: string read GetBody write fBody;
    property ContentType: string read fContentType write fContentType;
  end;

  { TRStompFrameTransaction }

  TRStompFrameTransaction = class(TRStompFrame)
  private
    fTxId: string;
  protected
    procedure DoAddHeaders(); override;
  published
    property TxId: string read fTxId write fTxId;
  end;

  { TRStompFrameSend }

  TRStompFrameSend = class(TRStompFrameTransaction)
  private
    fDestination: string;
  public
    constructor Create(Dest: string); reintroduce;
    destructor Destroy; override;
  end;

  { TRStompFrameAck }

  TRStompFrameAck = class(TRStompFrameTransaction)
  private
    fAckId: string;
  public
    constructor Create(AckId: string); reintroduce;
    destructor Destroy; override;
  published
    property AckId: string read fAckId;
  end;

  { TRStompFrameNAck }

  TRStompFrameNAck = class(TRStompFrameTransaction)
  private
    fAckId: string;
  public
    constructor Create(AckId: string); reintroduce;
    destructor Destroy(); override;
  published
    property AckId: string read fAckId;
  end;

  { TRStompFrameBegin }

  TRStompFrameBegin = class(TRStompFrameTransaction)
  public
    constructor Create(TransactionId: string); reintroduce;
  end;

  { TRStompFrameCommit }

  TRStompFrameCommit = class(TRStompFrameTransaction)
  public
    constructor Create(TransactionId: string); reintroduce;
  end;

  { TRStompFrameAbort }

  TRStompFrameAbort = class(TRStompFrameTransaction)
  public
    constructor Create(TransactionId: string); reintroduce;
  end;

  { TRStompFrameSubscribe }

  TRStompFrameSubscribe = class(TRStompFrame)
  private
    fSubscriptionId: string;
    fAckType: TRStompAckType;
    fDestination: string;
  protected
    procedure DoAddHeaders; override;
  public
    constructor Create(SubscriptionId, Dest: string; AckType: TRStompAckType = atAuto); reintroduce;
    destructor Destroy; override;
  published
    property SubscriptionId: string read fSubscriptionId write fSubscriptionId;
  end;

  { TRStompFrameUnsubscribe }

  TRStompFrameUnsubscribe = class(TRStompFrame)
  private
    fSubscriptionId: string;
  public
    constructor Create(SubscriptionId: string); reintroduce;
    destructor Destroy; override;
  end;

  { TRStompFrameConnect }

  TRStompFrameConnect = class(TRStompFrame)
  private
    fHeartBeat: string;
    fHost: string;
    fLogin: string;
    fPasscode: string;
    fProtocolVersion: string;
  public
    constructor Create; reintroduce;
    destructor Destroy; override;
  protected
    procedure DoAddHeaders(); override;
  published
    property ProtocolVersion: string read fProtocolVersion write fProtocolVersion;
    property Host: string read fHost write fHost;
    property Login: string read fLogin write fLogin;
    property Passcode: string read fPasscode write fPasscode;
    property HeartBeat: string read fHeartBeat write fHeartBeat;
  end;

  { TRStompFrameConnected }

  TRStompFrameConnected = class(TRStompFrame)
  private
    function GetSessionID: string;
    function GetVersion: string;
  public
    constructor Create; reintroduce;
    destructor Destroy; override;
  published
    property Version: string read GetVersion;
    property SessionID: string read GetSessionID;
  end;

  { TRStompFrameDisconnect }

  TRStompFrameDisconnect = class(TRStompFrame)
  public
    constructor Create(); reintroduce;
    destructor Destroy; override;
  end;

  { TRStompFrameMessage }

  TRStompFrameMessage = class(TRStompFrame)
  private
    function GetAckID: string;
    function GetDestination: string;
    function GetMessageID: string;
    function GetSubscriptionID: string;
  public
    constructor Create(); reintroduce;
    destructor Destroy; override;
  published
    property Destination: string read GetDestination;
    property MessageID: string read GetMessageID;
    property SubscriptionID: string read GetSubscriptionID;
    property AckID: string read GetAckID;
  end;

  { TRStompFrameError }

  TRStompFrameError = class(TRStompFrame)
  private
    function GetMsg: string;
    function GetReceiptID: string;
  public
    constructor Create(); reintroduce;
  published
    property Msg: string read GetMsg;
    property ReceiptID: string read GetReceiptID;
  end;

  { TRStompFrameReceipt }

  TRStompFrameReceipt = class(TRStompFrame)
  private
    function GetReceiptID: string;
  public
    constructor Create(); reintroduce;
  published
    property ReceiptID: string read GetReceiptID;
  end;

  { TRStompUtils }

  TRStompUtils = class
	public
    class function ClearList(List: TList): TObject;
    class function Escape(s: string): string;
    class function Unescape(s: string): string;
    class function CreateGUID(): string;
    class function ReplaceChars(s, replacedChars: string): string;
    class procedure Split(list: TStringList; s: string; delimiter: Char = #10);
  end;

  TRStompReceiveEvent = procedure(Data: string) of object;
  TRStompRecvParsedEvent = procedure(Frame: TRStompFrame) of object;

  RDataSendWait = record
    Destination: string;
    Listener: string;
    SubscriptionId: string;
    Body: string;
  end;

  TRStomp = class
  private
    fConnector: TIdTCPClient;
    fHeartBeat: string;
    fLogin: string;
    fOnRecvConnected: TRStompRecvParsedEvent;
    fOnRecvError: TRStompRecvParsedEvent;
    fOnRecvMessage: TRStompRecvParsedEvent;
    fOnRecvReceipt: TRStompRecvParsedEvent;
    fPasscode: string;
    fReceipt: string;
    fThread: TIdThreadComponent;
    fHost: string;
    fOnConnected: TNotifyEvent;
    fOnDisconnected: TNotifyEvent;
    fOnReceive: TRStompReceiveEvent;
    fPort: Word;
    fVhost: string;
    fMsg: string;
    fAckId: string;
    fBlocking: Boolean;

    procedure DoThreadRunEvent(Sender: TIdThreadComponent);
    procedure DoConnectorConnected(Sender: TObject);
    procedure DoConnectorDisconnected(Sender: TObject);
    procedure DoParseFrame(Frame: TRStompFrame; Raw: string);
    procedure DoWriteLnConsole();
    procedure DoSend(Frame: TRStompFrame);
    procedure DoSend(Frame: TRStompFrame; var Response: string);

    function CmdConnect(): Boolean;
    function CmdSend(Dest, Body: string): Boolean;
    function CmdSendWait(Destination, Listener, SubscriptionId, Body: string): string;
    function CmdSubscribe(SubscriptionId, Dest: string; AckType: TRStompAckType): Boolean;
    function CmdUnsubscribe(SubscriptionId: string): Boolean;
    function CmdDisconnect(): Boolean;
    function CmdBeginTx(TxId: string): Boolean;
    function CmdCommitTx(TxId: string): Boolean;
    function CmdAbortTx(TxId: string): Boolean;
    function CmdAck(AckId: string): Boolean;
    function CmdNAck(AckId: string): Boolean;
  public
    constructor Create(Host: string; Port: Word); reintroduce;
    destructor Destroy; override;

    function Connect(): Boolean;
    function Disconnect(): Boolean;
    function Send(Dest, Msg: string): Boolean;
    function SendWait(Data: RDataSendwait): string;
    function SendWait(Destination, Listener, SubscriptionId, Msg: string): string;
    function Subscribe(Id, Dest: string; AckType: TRStompAckType = atAuto): Boolean;
    function Unsubscribe(Id: string): Boolean;
    function BeginTx(TxId: string): Boolean;
    function CommitTx(TxId: string): Boolean;
    function Ack(): Boolean;
    function NAck(): Boolean;
  published
    property Host: string read fHost;
    property Port: Word read fPort;
    property Login: string read fLogin write fLogin;
    property HeartBeat: string read fHeartBeat write fHeartBeat;
    property Passcode: string read fPasscode write fPasscode;
    property Vhost: string read fVhost write fVhost;
    property Connector: TIdTCPClient read fConnector;
    property Receipt: string read fReceipt write fReceipt;
    property AckId: string read fAckId;

    property OnConnected: TNotifyEvent read fOnConnected write fOnConnected;
    property onDisconnected: TNotifyEvent read fOnDisconnected write fOnDisconnected;
    property OnReceive: TRStompReceiveEvent read fOnReceive write fOnReceive;
    property OnRecvConnected: TRStompRecvParsedEvent read fOnRecvConnected write fOnRecvConnected;
    property OnRecvMessage: TRStompRecvParsedEvent read fOnRecvMessage write fOnRecvMessage;
    property OnRecvReceipt: TRStompRecvParsedEvent read fOnRecvReceipt write fOnRecvReceipt;
    property OnRecvError: TRStompRecvParsedEvent read fOnRecvError write fOnRecvError;
  end;

const
  NULL = #0;
  LF = #10;
  CR = #13;
  EOL = LF;

  COMMANDS: array[TRStompCommand] of string = ('SEND', 'SUBSCRIBE', 'UNSUBSCRIBE', 'BEGIN',
  	'COMMIT', 'ABORT', 'ACK', 'NACK', 'DISCONNECT', 'CONNECT', 'STOMP', 'CONNECTED',
    'MESSAGE', 'RECEIPT', 'ERROR');
  ACK_TYPES: array[TRStompAckType] of string = ('auto', 'client', 'client-individual');

  ESCAPE_CHARS = ['"', '\', #10, #13, #9, #8, #12, #255];

  PROTOCOL_VERSION = '1.2';
  REQUEST_TIMEOUT = 2000;

var
  cs: TCriticalsection;

implementation

uses IdException;

{ TRStompFrameAbort }

constructor TRStompFrameAbort.Create(TransactionId: string);
begin
  inherited Create(scAbort);
  fTxId:= TransactionId;
end;

{ TRStompFrameCommit }

constructor TRStompFrameCommit.Create(TransactionId: string);
begin
  inherited Create(scCommit);
  fTxId:= TransactionId;
end;

{ TRStompFrameBegin }

constructor TRStompFrameBegin.Create(TransactionId: string);
begin
  inherited Create(scBegin);
  fTxId:= TransactionId;
end;

{ TRStompFrameReceipt }

function TRStompFrameReceipt.GetReceiptID: string;
var
  h: TRStompHeader;
begin
  h:= GetHeader('receipt-id');
  if h <> nil then
  	Result:= h.Value;
end;

constructor TRStompFrameReceipt.Create;
begin
  inherited Create(scReceipt);
end;

{ TRStompFrameError }

function TRStompFrameError.GetMsg: string;
var
  h: TRStompHeader;
begin
  h:= GetHeader('message');
	if h <> nil then
  	Result:= h.Value;
end;

function TRStompFrameError.GetReceiptID: string;
var
  h: TRStompHeader;
begin
  h:= GetHeader('receipt-id');
	if h <> nil then
  	Result:= h.Value;
end;

constructor TRStompFrameError.Create;
begin
  inherited Create(scError);
end;

{ TRStompFrameMessage }

function TRStompFrameMessage.GetAckID: string;
var
  h: TRStompHeader;
begin
  h:= GetHeader('ack');
  if h <> nil then
  	Result:= h.Value;
end;

function TRStompFrameMessage.GetDestination: string;
var
  h: TRStompHeader;
begin
  h:= GetHeader('destination');
  if h <> nil then
  	Result:= h.Value;
end;

function TRStompFrameMessage.GetMessageID: string;
var
  h: TRStompHeader;
begin
  h:= GetHeader('message-id');
  if h <> nil then
  	Result:= h.Value;
end;

function TRStompFrameMessage.GetSubscriptionID: string;
var
  h: TRStompHeader;
begin
  h:= GetHeader('subscription');
  if h <> nil then
  	Result:= h.Value;
end;

constructor TRStompFrameMessage.Create;
begin
  inherited Create(scMessage)
end;

destructor TRStompFrameMessage.Destroy;
begin
  inherited Destroy;
end;

{ TRStompFrameConnected }

function TRStompFrameConnected.GetVersion: string;
var
  h: TRStompHeader;
begin
  Result:= '';
  h:= GetHeader('version');
  if h <> nil then
  	Result := h.Value;
end;

function TRStompFrameConnected.GetSessionID: string;
var
  h: TRStompHeader;
begin
  Result:= '';
  h:= GetHeader('session');
  if h <> nil then
    Result:= h.Value;
end;

constructor TRStompFrameConnected.Create;
begin
  inherited Create(scConnected);
end;

destructor TRStompFrameConnected.Destroy;
begin
  inherited Destroy;
end;

{ TRStomp }

procedure TRStomp.DoThreadRunEvent(Sender: TIdThreadComponent);
var
  msg, cmd: String;
  frame: TRStompFrame;
begin
	if fBlocking then
  	Exit;

  try
    try
	    msg:= TrimLeft(fConnector.IOHandler.ReadLn(NULL));

      if Assigned(fOnReceive) then
      begin
  	    fOnReceive(msg);
        Exit;
      end;

      cmd:= Copy(msg, 1, Pos(LF, msg)-1);

      if Assigned(fOnRecvConnected) and AnsiSameText(cmd, COMMANDS[scConnected]) then
      begin
        frame:= TRStompFrameConnected.Create;
        DoParseFrame(frame, msg);
        fOnRecvConnected(frame);
      end
      else if Assigned(fOnRecvMessage) and AnsiSameText(cmd, COMMANDS[scMessage]) then
      begin
        frame:= TRStompFrameMessage.Create();
        DoParseFrame(frame, msg);
        fOnRecvMessage(frame);
      end
      else if Assigned(fOnRecvReceipt) and AnsiSameText(cmd, COMMANDS[scReceipt]) then
      begin
        frame:= TRStompFrameReceipt.Create();
        DoParseFrame(frame, msg);
        fOnRecvReceipt(frame);
      end
      else if Assigned(fOnRecvError) and AnsiSameText(cmd, COMMANDS[scError]) then
      begin
			  frame:= TRStompFrameError.Create();
        DoParseFrame(frame, msg);
        fOnRecvMessage(frame);
      end;
    except
      on E: Exception do
      begin
        fMsg:= E.Message;

        if E is EIdConnClosedGracefully then
				  fMsg:= 'Connection closed.';

        TThread.Queue(nil, @DoWriteLnConsole);
      end;
    end;
  finally
    if frame <> nil then
    begin
      fAckId:= frame.GetAckId();
    	FreeAndNil(frame);
    end;
  end;
end;

procedure TRStomp.DoConnectorConnected(Sender: TObject);
begin
  fThread.Active:= True;

  CmdConnect();

	if Assigned(fOnConnected) then
  	fOnConnected(Sender);
end;

procedure TRStomp.DoConnectorDisconnected(Sender: TObject);
begin
  if Assigned(fOnDisconnected) then
  	fOnDisconnected(Sender);
end;

procedure TRStomp.DoParseFrame(Frame: TRStompFrame; Raw: string);
var
  list: TStringList;
  posStart, posEnd: SizeInt;
  headers, header: String;
  i: Integer;
begin
  Raw:= AnsiReplaceText(Raw, CR, '');
  posStart:= Pos(EOL, Raw);
  posEnd:= Pos(EOL+EOL, Raw);
  headers:= Copy(Raw, posStart+1, posEnd-posStart-1);
  Frame.Body:= Copy(Raw, posEnd+2, Length(Raw));

  list:= TStringList.Create;
  try
  	TRStompUtils.Split(list, headers);
    frame.Headers.Clear;

    for i := 0 to list.Count-1 do
    begin
      header:= list[i];
      Frame.AddHeader(header);
    end;
  finally
    list.Free;
  end;
end;

procedure TRStomp.DoWriteLnConsole;
begin
  WriteLn(fMsg);
end;

procedure TRStomp.DoSend(Frame: TRStompFrame);
begin
  fReceipt:= '';
  fConnector.IOHandler.WriteLn(Frame.GetPacket());
end;

procedure TRStomp.DoSend(Frame: TRStompFrame; var Response: string);
var
  tick, timeout: QWord;
  raw, cmd: String;
begin
  Response:= '';
  DoSend(Frame);

  // Wait the response
  cmd:= '';
  raw:= '';
  tick:= GetTickCount64();
  timeout:= 0;
  while ((cmd <> 'MESSAGE') or (cmd <> 'ERROR')) and (timeout < REQUEST_TIMEOUT) do
  begin
		raw:= TrimLeft(fConnector.IOHandler.ReadLn(NULL));
    cmd:= Copy(raw, 1, Pos(LF, raw)-1);
    if cmd = 'MESSAGE' then
      Break
    else if cmd = 'ERROR' then
    	raise Exception.Create('Error response from server.');

    timeout:= GetTickCount64() - tick;
  end;

  Response:= raw;
end;

function TRStomp.CmdConnect: Boolean;
var
  f: TRStompFrameConnect;
begin
  Result:= True;

	f:= TRStompFrameConnect.Create;
  try
    f.ProtocolVersion:= PROTOCOL_VERSION;
    f.Login:= fLogin;
    f.Passcode:= fPasscode;
    f.HeartBeat:= fHeartBeat;
    f.Host:= fVhost;
    f.Receipt:= fReceipt;;

    try
  	  DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdSend(Dest, Body: string): Boolean;
var
  f: TRStompFrameSend;
begin
  Result:= True;
	f := TRStompFrameSend.Create(Dest);
  f.Body:= Body;
  f.Receipt:= fReceipt;

  try
    try
  		DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdSendWait(Destination, Listener, SubscriptionId, Body: string
  ): string;
var
  f: TRStompFrameSend;
begin
  Result:= '';

  cs.Acquire;
  try
  	fBlocking:= True;
  finally
  	cs.Release;
  end;

  Connect();
  Subscribe(SubscriptionId, Listener);

  f:= TRStompFrameSend.Create(Destination);
  f.Body:= Body;
  f.Receipt:= fReceipt;

  try
    DoSend(f, Result);
  finally
    Unsubscribe(SubscriptionId);
  	f.Free;

    try
    	fBlocking:= False;
    finally
    	cs.Release;
    end;
  end;
end;

function TRStomp.CmdSubscribe(SubscriptionId, Dest: string;
  AckType: TRStompAckType): Boolean;
var
  f: TRStompFrameSubscribe;
begin
  Result:= True;
	f := TRStompFrameSubscribe.Create(SubscriptionId, Dest, AckType);
  try
  	f.Receipt:= fReceipt;
    try
    	DoSend(f);
    except
    	Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdUnsubscribe(SubscriptionId: string): Boolean;
var
  f: TRStompFrameUnsubscribe;
begin
	f:= TRStompFrameUnsubscribe.Create(SubscriptionId);
  try
  	f.Receipt:= fReceipt;
    try
      DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdDisconnect: Boolean;
var
  f: TRStompFrameDisconnect;
begin
  Result:= True;
	f:= TRStompFrameDisconnect.Create();
  try
    f.Receipt:= fReceipt;
    try
    	DoSend(f);
    except
      Result:= False;
    end;
  finally
  	f.Free;
  end;
end;

function TRStomp.CmdBeginTx(TxId: string): Boolean;
var
  f: TRStompFrameBegin;
begin
	Result:= True;
  f:= TRStompFrameBegin.Create(TxId);
  f.Receipt:= fReceipt;
  try
    try
  		DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdCommitTx(TxId: string): Boolean;
var
  f: TRStompFrameCommit;
begin
  Result:= True;
  f:= TRStompFrameCommit.Create(TxId);
  try
  	try
    	f.Receipt:= fReceipt;
      DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdAbortTx(TxId: string): Boolean;
var
  f: TRStompFrameAbort;
begin
  Result:= True;
  f:= TRStompFrameAbort.Create(TxId);
  try
  	try
    	f.Receipt:= fReceipt;
      DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdAck(AckId: string): Boolean;
var
  f: TRStompFrameAck;
begin
  Result:= True;
  f:= TRStompFrameAck.Create(AckId);
  try
  	try
    	f.Receipt:= fReceipt;
      DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

function TRStomp.CmdNAck(AckId: string): Boolean;
var
  f: TRStompFrameNAck;
begin
  Result:= True;
  f:= TRStompFrameNAck.Create(AckId);
  try
  	try
    	f.Receipt:= fReceipt;
      DoSend(f);
    except
      Result:= False;
    end;
  finally
    f.Free;
  end;
end;

constructor TRStomp.Create(Host: string; Port: Word);
begin
  fBlocking:= False;
  fHost:= Host;
  fPort:= Port;

  fThread:= TIdThreadComponent.Create(nil);
  fThread.Loop:= True;
  fThread.OnRun:= @DoThreadRunEvent;

  fConnector:= TIdTCPClient.Create(nil);
  fConnector.OnConnected:= @DoConnectorConnected;
  fConnector.OnDisconnected:= @DoConnectorDisconnected;
end;

destructor TRStomp.Destroy;
begin
  if fThread.Active then
  	fThread.Active:= False;
  fThread.Free;

  fConnector.Free;
  inherited Destroy;
end;

function TRStomp.Connect: Boolean;
begin
  Result:= True;
  try
  	fConnector.Connect(fHost, fPort);
  except
    Result:= False;
  end;
end;

function TRStomp.Disconnect: Boolean;
begin
  Result:= CmdDisconnect();
end;

function TRStomp.Send(Dest, Msg: string): Boolean;
begin
  Result:= CmdSend(Dest, Msg);
end;

function TRStomp.SendWait(Data: RDataSendwait): string;
begin
  Result:= SendWait(Data.Destination, Data.Listener, Data.SubscriptionId, Data.Body);
end;

function TRStomp.SendWait(Destination, Listener, SubscriptionId, Msg: string
  ): string;
begin
  Result:= CmdSendWait(Destination, Listener, SubscriptionId, Msg);
end;

function TRStomp.Subscribe(Id, Dest: string; AckType: TRStompAckType): Boolean;
begin
  Result:= CmdSubscribe(Id, Dest, AckType);
end;

function TRStomp.Unsubscribe(Id: string): Boolean;
begin
  Result:= CmdUnsubscribe(Id);
end;

function TRStomp.BeginTx(TxId: string): Boolean;
begin
  Result:= CmdBeginTx(TxId);
end;

function TRStomp.CommitTx(TxId: string): Boolean;
begin
  Result:= CmdCommitTx(TxId);
end;

function TRStomp.Ack: Boolean;
begin
  Result:= False;
  if fAckId = '' then
  	Exit;

  Result:= CmdAck(fAckId);
end;

function TRStomp.NAck: Boolean;
begin
	Result:= False;
  if fAckId = '' then
  	Exit;

  Result:= CmdNAck(fAckId);
end;

{ TRStompFrameConnect }

constructor TRStompFrameConnect.Create;
begin
  inherited Create(scConnect);
end;

destructor TRStompFrameConnect.Destroy;
begin
  inherited Destroy;
end;

procedure TRStompFrameConnect.DoAddHeaders;
begin
  inherited DoAddHeaders;

  if fProtocolVersion = '' then
  	raise Exception.Create('Protocol version must be defined.');

  DoAddHeader('accept-version', fProtocolVersion);

  if fHost = '' then
    raise Exception.Create('Host is required and can''t be blank.');

  DoAddHeader('host', fHost);

  if fLogin <> '' then
  	DoAddHeader('login', fLogin);

  if fPasscode <> '' then
  	DoAddHeader('passcode', fPasscode);

  if fHeartBeat <> '' then
  	DoAddHeader('heart-beat', fHeartBeat);
end;

{ TRStompFrameDisconnect }

constructor TRStompFrameDisconnect.Create;
begin
  inherited Create(scDisconnect);
end;

destructor TRStompFrameDisconnect.Destroy;
begin
  inherited Destroy;
end;

{ TRStompFrameNAck }

constructor TRStompFrameNAck.Create(AckId: string);
begin
  inherited Create(scNAck);
  fAckId:= AckId;
  DoAddHeader('id', fAckId);
end;

destructor TRStompFrameNAck.Destroy;
begin
  inherited Destroy;
end;

{ TRStompFrameAck }

constructor TRStompFrameAck.Create(AckId: string);
begin
  inherited Create(scAck);
  fAckId:= AckId;
  DoAddHeader('id', fAckId);
end;

destructor TRStompFrameAck.Destroy;
begin
  inherited Destroy;
end;

{ TRStompFrameTransaction }

procedure TRStompFrameTransaction.DoAddHeaders;
begin
  inherited DoAddHeaders();

  if fTxId <> '' then
  	DoAddHeader('transaction', fTxId);
end;

{ TRStompFrameUnsubscribe }

constructor TRStompFrameUnsubscribe.Create(SubscriptionId: string);
begin
  inherited Create(scUnsubscribe);

  fSubscriptionId:= SubscriptionId;
  DoAddHeader('id', fSubscriptionId);
end;

destructor TRStompFrameUnsubscribe.Destroy;
begin
  inherited Destroy;
end;

{ TRStompFrameSubscribe }

procedure TRStompFrameSubscribe.DoAddHeaders;
begin
  inherited DoAddHeaders;

  if (fAckType = atClient) or (fAckType = atIndividual) then
  	DoAddHeader('ack', ACK_TYPES[fAckType]);
end;

constructor TRStompFrameSubscribe.Create(SubscriptionId, Dest: string;
  AckType: TRStompAckType);
begin
  inherited Create(scSubscribe);

  fSubscriptionId:= SubscriptionId;
  DoAddHeader('id', fSubscriptionId);

  fDestination:= Dest;
  DoAddHeader('destination', fDestination);

  fAckType:= AckType;
  DoAddHeader('ack', ACK_TYPES[fAckType]);
end;

destructor TRStompFrameSubscribe.Destroy;
begin
  inherited Destroy;
end;

{ TRStompFrameSend }

constructor TRStompFrameSend.Create(Dest: string);
begin
  inherited Create(scSend);

  fDestination:= Dest;
  DoAddHeader('destination', fDestination);
end;

destructor TRStompFrameSend.Destroy;
begin
  inherited Destroy;
end;

{ TRStompUtils }

class function TRStompUtils.ClearList(List: TList): TObject;
var
  i: Integer;
  o: TObject;
begin
  Result:= List;
  for i:= 0 to List.Count-1 do
  begin
  	o:= TObject(List[i]);
    o.Free;
  end;
end;

class function TRStompUtils.Escape(s: string): string;
var
  len, i: Integer;
  c: Char;
  ps: PChar;
  escapeChar: string;
begin
  Result := '';
  len := Length(s);
  ps := PChar(s);
  for i := 0 to len-1 do
  begin
    c := ps[i];
    escapeChar := c;
    if c in ESCAPE_CHARS then
    begin
      if c = #10 then
        escapeChar := 'n'
      else if c = #13 then
        escapeChar := 'r'
      else if c = #9 then
        escapeChar := 't'
      else if c = #8 then
        escapeChar := 'b'
      else if c = #12 then
        escapeChar := 'f'
      else if c = #255 then
        escapeChar := 'xFF'
      else
        escapeChar := c;

      escapeChar := '\' + escapeChar;
    end;

    Result := Result + escapeChar;
  end;
end;

class function TRStompUtils.Unescape(s: string): string;

function getEscapeChar(var len: Integer; i: Integer; s: PChar): Char;
var
  l: Integer;
begin
  Result := #0;
  l := 0;
  if i <= Length(string(s)) - 1 then
  begin
    l := 1;
    if s[i+1] = 'x' then
    begin
      l := 3;
      Result := #255
    end
    else if s[i+1] = 'n' then
      Result := #10
    else if s[i+1] = 'r' then
      Result := #13
    else if s[i+1] = 't' then
      Result := #9
    else if s[i+1] = 'b' then
      Result := #8
    else if s[i+1] = 'f' then
      Result := #12
    else if s[i+1] = '\' then
      Result := getEscapeChar(len, i+1, s)
    else
      Result := s[i+1]
  end;
  len := len + l;
end;

var
  i, len: Integer;
  c, escapeChar: Char;
  ps: PChar;
begin
  Result := '';
  i := 0;
  s := s + #3;
  c := #0;
  ps := PChar(s);
  while c <> #3 do
  begin
    c := ps[i];
    if c = '\' then
    begin
      len := 0;
      escapeChar := getEscapeChar(len, i, ps);
      Result := Result + escapeChar;
      Inc(i, len+1)
    end
    else begin
      Result := Result + c;
      Inc(i);
    end;
  end;
  Result := Copy(Result, 1, Length(Result)-1);
end;

class function TRStompUtils.CreateGUID(): string;
var
  guid: TGUID;
  code: Integer;
begin
  Result:= '';
  code:= SysUtils.CreateGUID(guid);
  if code <> 0 then
  	Exit;

  Result:= GUIDToString(guid);
  Result:= LowerCase(ReplaceChars(Result, '{-}'));
end;

class function TRStompUtils.ReplaceChars(s, replacedChars: string): string;
var
  i: Integer;
  l: SizeInt;
  ch: Char;
begin
  Result:= '';
  l:= Length(s);
  for i:= 0 to l - 1 do
  begin
    ch:= s[i+1];
  	if Pos(ch, replacedChars) > 0 then
    	Continue;

    Result:= Result + ch;
  end;
end;

class procedure TRStompUtils.Split(list: TStringList; s: string; delimiter: Char
  );
var
  i, len, p, pClosed: Integer;
  closed, found: Boolean;
  c: Char;
  left, right: string;
begin
  right := s;
  len := Length(right);
  if len = 0 then
    Exit;

  if PChar(right)[len-1] <> delimiter then
    right := right + delimiter;

  closed := True;
  found := False;
  pClosed := 0;
  len := Length(right);
  for i := 0 to len - 1 do
  begin
    c := PChar(right)[i];
    if c = '"' then
    begin
      closed := not closed;
      if closed then
        pClosed := i+1
    end;

    if (c = delimiter) and closed then
    begin
      if not found then
        p := Pos(delimiter, right)
      else begin
        p := pClosed+1;
      end;

      left := Trim(Copy(right, 1, p-1));
      list.Add(left);

      right := Copy(right, p+1, len);
      Split(list, right, delimiter);

      Break;
    end
    else if (c = delimiter) and not closed then
      found := True;
  end;
end;

{ TRStompFrame }

procedure TRStompFrame.DoAddHeader(Name, Value: string);
var
  header: TRStompHeader;
begin
  header:= TRStompHeader.Create;
  header.Name:= Name;
  header.Value:= Value;
  fHeaders.Add(header);
end;

procedure TRStompFrame.DoAddHeaders;
var
  len: Integer;
begin
  len:= Length(fBody);
  if len > 0 then
  begin
    DoAddHeader('content-type', fContentType);
  	DoAddHeader('content-length', IntToStr(len));
  end;

  if fReceipt <> '' then
  	DoAddHeader('receipt', fReceipt);
end;

constructor TRStompFrame.Create(Command: TRStompCommand);
begin
  fCommand:= Command;
  fHeaders:= TList.Create;
  fContentType:= 'text/plain';
end;

destructor TRStompFrame.Destroy;
begin
  TRStompUtils.ClearList(fHeaders).Free;
  inherited Destroy;
end;

function TRStompFrame.GetPacket: string;
begin
  if not (fCommand in [scConnected, scMessage, scError, scReceipt]) then
		DoAddHeaders();

  {
  	STOMP frame follow this following structure:
  			COMMAND
        header1:value1
        header2: value2

        Body^@

  }

  Result:= 	COMMANDS[fCommand] + EOL +
  					GetHeaderString() +
            EOL +
            GetBody() + NULL +
            EOL
end;

function TRStompFrame.GetHeaderString: string;
var
  i: Integer;
  h: TRStompHeader;
begin
  Result:= '';
	for i:= 0 to fHeaders.Count-1 do
  begin
  	h:= TRStompHeader(fHeaders[i]);
    Result:= Result + h.GetString();
  end;
end;

function TRStompFrame.GetBody: string;
begin
  Result := fBody;
end;

function TRStompFrame.GetRawFrame: string;
begin
  Result:= fRawFrame;;
end;

function TRStompFrame.GetHeader(HeaderName: string): TRStompHeader;
var
  i: Integer;
  h: TRStompHeader;
begin
  Result:= nil;
  for i := 0 to fHeaders.Count-1 do
  begin
  	h:= TRStompHeader(fHeaders[i]);
    if AnsiSameText(HeaderName, h.Name) then
    begin
      Result := h;
      Exit;
    end;
  end;
end;

function TRStompFrame.GetAckId: string;
var
  h: TRStompHeader;
begin
  Result:= '';

  h:= GetHeader('ack');
  if h = nil then
  	Exit;

  Result:= h.Value;
end;

procedure TRStompFrame.SetRawFrame(Value: string);
begin
  fRawFrame:= Value;;
end;

procedure TRStompFrame.AddHeader(Name, Value: string);
begin
  DoAddHeader(Name, Value);
end;

procedure TRStompFrame.AddHeader(Header: string);
var
  h: TRStompHeader;
begin
	h:= TRStompHeader.Create(Header);
  fHeaders.Add(h);
end;

{ TRStompHeader }

constructor TRStompHeader.Create(Header: string);
var
  p: SizeInt;
begin
  p:= Pos(':', Header);
	fName:= Copy(Header, 1, p-1);
  fValue:= Copy(Header, p+1, Length(Header));
end;

function TRStompHeader.GetString: string;
begin
  Result := fName + ':' + fValue + EOL;
end;

initialization

cs:= TCriticalSection.Create;

finalization

FreeAndNil(cs);

end.


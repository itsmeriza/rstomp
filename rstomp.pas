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
  Classes, SysUtils;

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
    function GetString(): string;
  published
    property Name: string read fName write fName;
    property Value: string read fValue write fValue;
  end;

  { TRStompFrame }

  TRStompFrame = class
  private
    fMsgId: string;
    fTransactionId: string;
  protected
    fBody: string;
    fCommand: TRStompCommand;
    fHeaders: TList;

    procedure DoAddHeader(Name, Value: string);
    procedure DoAddHeaders(); virtual;
  public
    constructor Create(Command: TRStompCommand);
    destructor Destroy; override;

    function GetPacket(): string;
    function GetHeaderString: string;
    function GetBody(): string;
 	published
    property MsgId: string read fMsgId write fMsgId;
  	property Command: TRStompCommand read fCommand write fCommand;
    property Headers: TList read fHeaders write fHeaders;
    property Body: string read fBody write fBody;
    property TransactionId: string read fTransactionId write fTransactionId;
  end;

  { TRStompFrameSend }

  TRStompFrameSend = class(TRStompFrame)
  private
    fDestination: string;
  public
    constructor Create(Dest: string); reintroduce;
    destructor Destroy; override;
  end;

  { TRStompFrameSubscribe }

  TRStompFrameSubscribe = class(TRStompFrame)
  private
    fSubscriptionId: string;
    fAckType: TRStompAckType;
    fDestination: string;
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

  { TRStompUtils }

  TRStompUtils = class
	public
    class function ClearList(List: TList): TObject;
    class function Escape(s: string): string;
    class function Unescape(s: string): string;
    class function CreateGUID(): string;
  end;

  TRStomp = class
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


implementation

{ TRStompFrameUnsubscribe }

constructor TRStompFrameUnsubscribe.Create(SubscriptionId: string);
begin
  fSubscriptionId:= SubscriptionId;
  DoAddHeader('id', fSubscriptionId);
end;

destructor TRStompFrameUnsubscribe.Destroy;
begin
  inherited Destroy;
end;

{ TRStompFrameSubscribe }

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

class function TRStompUtils.CreateGUID: string;
var
  code: Integer;
  guid: TGUID;
begin
  Result:= '';
  code:= SysUtils.CreateGUID(guid);
  if code <> 0 then
		Result:= GUIDToString(guid).Replace('-', '');
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
    DoAddHeader('content-type', 'text/plain');
  	DoAddHeader('content-length', IntToStr(len));
  end;

  if fTransactionId <> '' then
  	DoAddHeader('transaction', fTransactionId);

  if fMsgId <> '' then
  	DoAddHeader('receipt', fMsgId);
end;

constructor TRStompFrame.Create(Command: TRStompCommand);
begin
  fCommand:= Command;
  fMsgId:= MsgId;
  fHeaders:= TList.Create;
end;

destructor TRStompFrame.Destroy;
begin
  TRStompUtils.ClearList(fHeaders).Free;
  inherited Destroy;
end;

function TRStompFrame.GetPacket: string;
begin
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
	for i:= 0 to fHeaders.Count do
  begin
  	h:= TRStompHeader(fHeaders[i]);
    Result:= Result + h.GetString();
  end;
end;

function TRStompFrame.GetBody: string;
begin
  Result := fBody;
end;

{ TRStompHeader }

function TRStompHeader.GetString: string;
begin
  Result := fName + ':' + fValue + EOL;
end;

end.


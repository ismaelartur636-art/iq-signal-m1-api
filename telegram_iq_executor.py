"""MEGA IA Telegram -> IQ Option executor.
Reads MEGA_EXEC|v1 messages from one Telegram group using a Telegram USER session.
Defaults to PRACTICE. Set IQ_ACCOUNT=REAL only after validating the complete flow.
"""
import os, re, time, asyncio
from datetime import datetime, timezone
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from iqoptionapi.stable_api import IQ_Option

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_STRING_SESSION"]
GROUP_ID = int(os.environ["TG_GROUP_ID"])
IQ_EMAIL = os.environ["IQ_EMAIL"]
IQ_PASSWORD = os.environ["IQ_PASSWORD"]
IQ_ACCOUNT = os.getenv("IQ_ACCOUNT", "PRACTICE").upper()
IQ_AMOUNT = float(os.getenv("IQ_AMOUNT", "2"))
MAX_LATE_SECONDS = int(os.getenv("MAX_LATE_SECONDS", "8"))

LINE = re.compile(r"^MEGA_EXEC\|v1\|([A-Z0-9-]+)\|(CALL|PUT)\|([^|]+)\|([^|]+)\|(OPEN|IQ_OTC)\|(.+)$", re.M)
seen = set()
iq = None
lock = asyncio.Lock()

def parse_dt(v):
    d=datetime.fromisoformat(v.replace("Z", "+00:00"))
    if d.tzinfo is None: d=d.replace(tzinfo=timezone.utc)
    return d

def connect_iq():
    global iq
    if iq:
        try:
            if iq.check_connect(): return iq
        except Exception: pass
        try: iq.close()
        except Exception: pass
    c=IQ_Option(IQ_EMAIL, IQ_PASSWORD)
    ok, reason=c.connect()
    if not ok: raise RuntimeError(f"IQ login recusado: {reason}")
    c.change_balance(IQ_ACCOUNT)
    iq=c
    return c

def active_candidates(symbol, market):
    base=symbol.replace("/", "").upper()
    if market == "IQ_OTC": return [base+"-OTC", base+"-OTC-op"]
    return [base]

def place_binary(symbol, direction, entry_iso, expiry_iso, market):
    c=connect_iq()
    entry=parse_dt(entry_iso); expiry=parse_dt(expiry_iso)
    now=datetime.now(timezone.utc)
    late=(now-entry.astimezone(timezone.utc)).total_seconds()
    if late > MAX_LATE_SECONDS: raise RuntimeError(f"sinal atrasado {late:.1f}s; ordem bloqueada")
    if (expiry-entry).total_seconds() < 30: raise RuntimeError("expiração inválida")
    action="call" if direction=="CALL" else "put"
    expiry_epoch=int(expiry.timestamp())
    errors=[]
    for active in active_candidates(symbol, market):
        try:
            fn=getattr(c, "buy_by_raw_expirations", None)
            if callable(fn): result=fn(IQ_AMOUNT, active, action, expiry_epoch)
            else:
                mins=max(1, round((expiry-entry).total_seconds()/60))
                result=c.buy(IQ_AMOUNT, active, action, mins)
            ok, oid = (result[0], result[1]) if isinstance(result,(tuple,list)) and len(result)>=2 else (bool(result), result)
            if ok: return active, oid
            errors.append(f"{active}: {result}")
        except Exception as e: errors.append(f"{active}: {e}")
    raise RuntimeError(" | ".join(errors[-3:]))

client=TelegramClient(StringSession(SESSION), API_ID, API_HASH)

@client.on(events.NewMessage(chats=GROUP_ID))
async def on_signal(event):
    m=LINE.search(event.raw_text or "")
    if not m: return
    symbol,direction,entry,expiry,market,event_id=m.groups()
    if event_id in seen: return
    seen.add(event_id)
    if len(seen)>1000:
        # bounded in-memory dedupe; Telegram event id also prevents ordinary replay
        seen.clear(); seen.add(event_id)
    async with lock:
        try:
            active, oid=await asyncio.to_thread(place_binary,symbol,direction,entry,expiry,market)
            print(f"[EXEC OK] {active} {direction} amount={IQ_AMOUNT} account={IQ_ACCOUNT} order={oid}", flush=True)
        except Exception as e:
            print(f"[EXEC BLOCK] {symbol} {direction}: {e}", flush=True)

async def main():
    await client.start()
    print(f"[MEGA EXEC] Telegram ativo • grupo={GROUP_ID} • IQ={IQ_ACCOUNT} • valor={IQ_AMOUNT}", flush=True)
    await client.run_until_disconnected()

if __name__ == "__main__": asyncio.run(main())

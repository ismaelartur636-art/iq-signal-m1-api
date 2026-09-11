import os, asyncio, time, json, re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

app=FastAPI(title='MEGA IA',version='12.0.0')
BR_TZ=ZoneInfo('America/Sao_Paulo'); UTC=timezone.utc
TD_KEY=os.getenv('TWELVE_DATA_API_KEY','').strip(); OAI_KEY=os.getenv('OPENAI_API_KEY','').strip()
OAI_MODEL=os.getenv('OPENAI_MODEL','gpt-5.6-luna'); OAI_MIN=float(os.getenv('OPENAI_MIN_CONFIDENCE','70')); OAI_TIMEOUT=float(os.getenv('OPENAI_TIMEOUT','15'))
LICENSE=os.getenv('LICENSE_EXPIRES','2026-12-31'); WA1=os.getenv('WHATSAPP_1','55 84 99841-1282'); WA2=os.getenv('WHATSAPP_2','55 84 99449-9442'); IG=os.getenv('INSTAGRAM','@Ismaelartur26')
TD_URL='https://api.twelvedata.com/time_series'; OAI_URL='https://api.openai.com/v1/responses'
INTERVALS={'1min':60,'5min':300,'15min':900,'30min':1800}
SYMBOLS=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','NZD/USD','EUR/JPY','GBP/JPY','EUR/GBP','BTC/USD','ETH/USD']
cache={}; oai_cache={}; results={}

def now(): return datetime.now(BR_TZ)
def iso(d): return d.astimezone(BR_TZ).isoformat()
def parse(s):
 d=datetime.fromisoformat(s.replace('Z','+00:00')); return (d if d.tzinfo else d.replace(tzinfo=UTC)).astimezone(BR_TZ)
def clamp(x,a,b): return max(a,min(b,x))
def ema(v,p):
 if len(v)<p:return None
 k=2/(p+1); e=sum(v[:p])/p
 for x in v[p:]:e=x*k+e*(1-k)
 return e
def rsi(v,p=14):
 if len(v)<p+1:return None
 g=[];l=[]
 for i in range(1,len(v)):
  d=v[i]-v[i-1];g.append(max(d,0));l.append(max(-d,0))
 ag=sum(g[-p:])/p;al=sum(l[-p:])/p
 return 100 if al==0 else 100-100/(1+ag/al)

async def candles(symbol,interval,n=80):
 if not TD_KEY: raise HTTPException(500,'TWELVE_DATA_API_KEY não configurada.')
 params={'symbol':symbol,'interval':interval,'outputsize':n,'apikey':TD_KEY,'format':'JSON'}
 async with httpx.AsyncClient(timeout=15) as c:r=await c.get(TD_URL,params=params)
 r.raise_for_status(); data=r.json()
 if data.get('status')=='error':raise HTTPException(502,data.get('message','Erro Twelve Data.'))
 out=[]
 for x in reversed(data.get('values',[])):
  try:out.append({'datetime':x['datetime'],'open':float(x['open']),'high':float(x['high']),'low':float(x['low']),'close':float(x['close']),'volume':float(x.get('volume',0) or 0)})
  except:pass
 if not out:raise HTTPException(502,'Nenhum candle recebido.')
 return out

def local_ai(cs):
 if len(cs)<35:return {'direction':'NEUTRO','confidence':0,'confirmed':False}
 v=[c['close'] for c in cs]; a=ema(v,3);b=ema(v,7);q=rsi(v,14);last,prev=cs[-1],cs[-2];votes={'CALL':0,'PUT':0}
 if a and b:votes['CALL' if a>b else 'PUT']+=1
 if q is not None:
  if q<=35:votes['CALL']+=1.2
  elif q>=65:votes['PUT']+=1.2
  elif q>50:votes['CALL']+=.4
  elif q<50:votes['PUT']+=.4
 if last['close']>prev['close']:votes['CALL']+=.7
 elif last['close']<prev['close']:votes['PUT']+=.7
 ratio=abs(last['close']-last['open'])/max(last['high']-last['low'],1e-12)
 if ratio>=.6:votes['CALL' if last['close']>last['open'] else 'PUT']+=.7
 d='CALL' if votes['CALL']>votes['PUT'] else 'PUT' if votes['PUT']>votes['CALL'] else 'NEUTRO';total=sum(votes.values())
 conf=50+abs(votes['CALL']-votes['PUT'])/total*45 if total else 0
 return {'direction':d,'confidence':round(clamp(conf,0,97),1),'confirmed':conf>=65}

def json_extract(s):
 s=re.sub(r'^```(?:json)?\s*|\s*```$','',s.strip(),flags=re.I)
 try:return json.loads(s)
 except:pass
 m=re.search(r'\{.*\}',s,re.S)
 try:return json.loads(m.group(0)) if m else None
 except:return None

async def openai_confirm(symbol,interval,cs,local):
 if not OAI_KEY:return {'available':False}
 key=f'{symbol}|{interval}|{cs[-1]["datetime"]}'
 if key in oai_cache and time.time()-oai_cache[key][0]<55:return oai_cache[key][1]
 data=[{'time':c['datetime'],'o':c['open'],'h':c['high'],'l':c['low'],'c':c['close'],'v':c['volume']} for c in cs[-40:]]
 prompt=f'''Você é o módulo de confirmação da MEGA IA. Ativo {symbol}, timeframe {interval}. Use somente candles fechados. Analise tendência, momentum, estrutura, força, volatilidade e reversão. Não invente dados futuros. Direção preliminar {local["direction"]}, confiança {local["confidence"]}. Retorne somente JSON: {{"direction":"CALL|PUT|NEUTRO","confidence":0,"confirmed":true,"reason":"curto","risk":"LOW|MEDIUM|HIGH"}} Candles: {json.dumps(data)}'''
 try:
  async with httpx.AsyncClient(timeout=OAI_TIMEOUT) as c:r=await c.post(OAI_URL,headers={'Authorization':f'Bearer {OAI_KEY}','Content-Type':'application/json'},json={'model':OAI_MODEL,'input':prompt})
  r.raise_for_status();d=r.json();text=d.get('output_text','')
  if not text:
   for it in d.get('output',[]):
    for co in it.get('content',[]):
     if co.get('type') in ('output_text','text'):text+=co.get('text','')
  p=json_extract(text)
  if not isinstance(p,dict):raise ValueError()
  out={'available':True,'direction':str(p.get('direction','NEUTRO')).upper(),'confidence':clamp(float(p.get('confidence',0)),0,100),'confirmed':bool(p.get('confirmed',False)),'risk':str(p.get('risk','HIGH')).upper()}
  if out['direction'] not in ('CALL','PUT','NEUTRO'):out['direction']='NEUTRO'
  oai_cache[key]=(time.time(),out);return out
 except:return {'available':False}

def next_entry(interval):
 s=INTERVALS[interval];t=int(now().timestamp());return datetime.fromtimestamp(((t//s)+1)*s,tz=BR_TZ)

async def signal(symbol,interval):
 key=f'{symbol}|{interval}'
 if key in cache and time.time()-cache[key][0]<4:return cache[key][1]
 cs=(await candles(symbol,interval,80))[:-1];local=local_ai(cs);entry=next_entry(interval);expiry=entry+timedelta(seconds=INTERVALS[interval])
 base={'symbol':symbol,'interval':interval,'direction':'NEUTRO','confidence':local['confidence'],'entry_time':iso(entry),'expiry_time':iso(expiry),'status':'AGUARDANDO','ai_confirmed':False,'risk':'HIGH'}
 if local['confirmed']:
  ai=await openai_confirm(symbol,interval,cs,local)
  if ai.get('available') and ai['direction']==local['direction'] and ai['confirmed'] and ai['confidence']>=OAI_MIN and ai.get('risk')!='HIGH':
   base.update(direction=local['direction'],confidence=round(clamp(local['confidence']*.45+ai['confidence']*.55,0,97),1),status='SINAL LIBERADO',ai_confirmed=True,risk=ai.get('risk','MEDIUM'))
  elif not ai.get('available'):
   base.update(direction=local['direction'],status='SINAL LOCAL',ai_confirmed=False,risk='MEDIUM')
  else:base.update(confidence=round(min(local['confidence'],ai['confidence']),1),status='AGUARDANDO CONFIRMAÇÃO',risk=ai.get('risk','HIGH'))
 cache[key]=(time.time(),base);return base

@app.get('/health')
async def health():return {'status':'ok','app':'MEGA IA','version':'12.0.0','brasilia_time':iso(now()),'twelve_data_configured':bool(TD_KEY),'openai_configured':bool(OAI_KEY)}
@app.get('/server-time')
async def server_time():return {'datetime':iso(now()),'timezone':'America/Sao_Paulo'}
@app.get('/license')
async def license_info():
 try:e=datetime.strptime(LICENSE,'%Y-%m-%d').date();d=max(0,(e-now().date()).days);active=now().date()<=e
 except:active=False;d=0
 return {'active':active,'expires':LICENSE,'days_remaining':d,'whatsapp_1':WA1,'whatsapp_2':WA2,'instagram':IG}
@app.get('/candles')
async def get_candles(symbol='EUR/USD',interval='1min',limit=50):
 if interval not in INTERVALS:raise HTTPException(400,'Intervalo inválido.')
 return {'symbol':symbol,'interval':interval,'candles':await candles(symbol,interval,int(clamp(limit,10,100)))}
@app.get('/signal-ai')
async def signal_ai(symbol='EUR/USD',interval='1min'):
 if symbol not in SYMBOLS or interval not in INTERVALS:raise HTTPException(400,'Ativo ou intervalo inválido.')
 return await signal(symbol,interval)
@app.get('/signal')
async def get_signal(symbol='EUR/USD',interval='1min'):return await signal(symbol,interval)
@app.get('/ai-analysis')
async def ai_analysis(symbol='EUR/USD',interval='1min'):
 s=await signal(symbol,interval);return {k:s.get(k) for k in ('symbol','interval','direction','confidence','status','ai_confirmed','risk')}
@app.get('/radar')
async def radar(interval='1min'):
 vals=await asyncio.gather(*(signal(s,interval) for s in SYMBOLS),return_exceptions=True)
 return [{'symbol':s,'direction':'NEUTRO' if isinstance(v,Exception) else v['direction'],'confidence':0 if isinstance(v,Exception) else v['confidence'],'status':'SEM DADOS' if isinstance(v,Exception) else v['status']} for s,v in zip(SYMBOLS,vals)]
@app.get('/sniper-ranking')
async def ranking():return [{'name':x,'score':0} for x in ('Modelo A','Modelo B','Modelo C','Modelo D')]
@app.get('/performance')
async def performance(interval='1min'):
 w=sum(1 for x in results.values() if x.get('result')=='WIN');l=sum(1 for x in results.values() if x.get('result')=='LOSS');t=w+l
 return {'wins':w,'losses':l,'total':t,'accuracy':round(w/t*100,2) if t else 0}
@app.get('/result')
async def result(symbol='EUR/USD',interval='1min',direction='CALL',expiry_time=''):
 if not expiry_time:raise HTTPException(400,'expiry_time é obrigatório.')
 key=f'{symbol}|{interval}|{direction}|{expiry_time}'
 if key in results:return results[key]
 if now()<parse(expiry_time):return {'status':'PENDENTE','result':None}
 cs=await candles(symbol,interval,20);target=None
 for c in cs:
  try:
   if parse(c['datetime'])>=parse(expiry_time):target=c;break
  except:pass
 if not target:return {'status':'AGUARDANDO CANDLE','result':None}
 direction=direction.upper();res='WIN' if ((direction=='CALL' and target['close']>target['open']) or (direction=='PUT' and target['close']<target['open'])) else 'LOSS'
 out={'status':'FINALIZADA','result':res,'candle_time':target['datetime']};results[key]=out;return out

@app.get('/',response_class=HTMLResponse)
async def home():
 return HTMLResponse('''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MEGA IA</title><style>body{margin:0;background:#070b12;color:#eaf2ff;font-family:Arial}.wrap{max-width:1100px;margin:auto;padding:18px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:14px}.card{background:#0e1522;border:1px solid #1c2a3e;border-radius:18px;padding:16px}.signal{grid-column:span 2;text-align:center;min-height:220px}.big{font-size:32px;font-weight:bold;margin:8px}.call{color:#4cff9b}.put{color:#ff5c7a}.neutral{color:#ffd166}.controls{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}select,button{background:#111d2e;color:#fff;border:1px solid #2a3d59;border-radius:12px;padding:11px}.radar{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.radar div{background:#101a29;padding:10px;border-radius:12px}.label,small{color:#8291a8;font-size:11px}@media(max-width:700px){.grid{grid-template-columns:1fr 1fr}.signal{grid-column:span 2}.radar{grid-template-columns:1fr 1fr}}@media(max-width:450px){.grid{grid-template-columns:1fr}.signal{grid-column:span 1}}</style></head><body><div class="wrap"><h1>🤖 MEGA IA</h1><small>ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</small><div id="clock"></div><div class="controls"><select id="symbol"></select><select id="interval"><option>1min</option><option>5min</option><option>15min</option><option>30min</option></select><button onclick="voice()">🔊 Ativar voz</button></div><div class="grid"><div class="card signal"><div style="font-size:65px">🤖</div><div class="label">SINAL ATUAL</div><div id="direction" class="big neutral">AGUARDANDO</div><div id="confidence">Confiança: --</div></div><div class="card"><div class="label">ENTRADA</div><div id="entry" class="big">--:--:--</div><div id="countdown">--</div></div><div class="card"><div class="label">STATUS IA</div><div id="status" class="big" style="font-size:20px">MONITORANDO</div><div id="risk">Risco: --</div></div></div><div class="grid"><div class="card"><div class="label">WIN</div><div id="wins" class="big call">0</div></div><div class="card"><div class="label">LOSS</div><div id="losses" class="big put">0</div></div><div class="card"><div class="label">ASSERTIVIDADE</div><div id="accuracy" class="big">0%</div></div><div class="card"><div class="label">RESULTADO</div><div id="result" class="big">--</div></div></div><div class="card" style="margin-top:12px"><b>Radar de oportunidades</b><div id="radar" class="radar" style="margin-top:12px"></div></div><div class="card" style="margin-top:12px"><div class="label">LICENÇA</div><div id="license">Verificando...</div></div></div><script>const syms=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','NZD/USD','EUR/JPY','GBP/JPY','EUR/GBP','BTC/USD','ETH/USD'];const S=document.getElementById('symbol');syms.forEach(x=>S.add(new Option(x,x)));let cur=null,v=false,last='',five=false,entered=false,reskey='';function speak(t){if(!v||!speechSynthesis)return;speechSynthesis.cancel();let u=new SpeechSynthesisUtterance(t);u.lang='pt-BR';speechSynthesis.speak(u)}function voice(){v=true;speak('Voz da Mega IA ativada.')}function ft(x){return x?new Date(x).toLocaleTimeString('pt-BR',{hour12:false}):'--:--:--'}async function get(u){let r=await fetch(u,{cache:'no-store'});return r.json()}async function sig(){try{cur=await get(`/signal-ai?symbol=${encodeURIComponent(S.value)}&interval=${interval.value}`);let d=document.getElementById('direction');d.textContent=cur.direction;d.className='big '+(cur.direction==='CALL'?'call':cur.direction==='PUT'?'put':'neutral');confidence.textContent='Confiança: '+cur.confidence+'%';entry.textContent=ft(cur.entry_time);status.textContent=cur.status;risk.textContent='Risco: '+cur.risk;let k=cur.symbol+'|'+cur.entry_time+'|'+cur.direction;if(k!==last&&cur.direction!=='NEUTRO'){last=k;speak(`Atenção. A Mega IA encontrou uma oportunidade no ${cur.symbol.replace('/',' ')}. Sinal ${cur.direction}. Entrada programada para ${ft(cur.entry_time)}.`)}}catch(e){status.textContent='ERRO DE DADOS'}}async function perf(){try{let p=await get('/performance');wins.textContent=p.wins;losses.textContent=p.losses;accuracy.textContent=p.accuracy+'%'}catch(e){}}async function rad(){try{let a=await get('/radar?interval='+interval.value);document.getElementById('radar').innerHTML=a.map(x=>`<div><b>${x.symbol}</b><br><span class="${x.direction==='CALL'?'call':x.direction==='PUT'?'put':'neutral'}">${x.direction}</span> • ${x.confidence}%<br><small>${x.status}</small></div>`).join('')}catch(e){}}async function lic(){let x=await get('/license');license.textContent=x.active?`● LICENÇA ATIVA • ${x.expires} • ${x.days_remaining} dias restantes`:`● LICENÇA EXPIRADA • ${x.whatsapp_1} / ${x.whatsapp_2} • ${x.instagram}`}async function clk(){let x=await get('/server-time');clock.textContent=ft(x.datetime)+' • Brasília'}function cd(){if(!cur)return;let n=Math.ceil((new Date(cur.entry_time)-Date.now())/1000);countdown.textContent=n>0?'Entrada em '+n+'s':'Entrada liberada';if(n===5&&!five){five=true;speak('Atenção. Entrada em 5 segundos.')}if(n<=0&&n>-2&&!entered){entered=true;speak('Entrada liberada. '+cur.direction+' agora.')}}async function resultCheck(){if(!cur||cur.direction==='NEUTRO')return;try{let x=await get(`/result?symbol=${encodeURIComponent(cur.symbol)}&interval=${cur.interval}&direction=${cur.direction}&expiry_time=${encodeURIComponent(cur.expiry_time)}`);if(x.result){result.textContent=x.result;let k=cur.symbol+'|'+cur.expiry_time;if(k!==reskey){reskey=k;speak('Operação finalizada. Resultado '+x.result+'.')}perf()}}catch(e){}}S.onchange=()=>{last='';five=false;entered=false;sig()};interval.onchange=()=>{last='';five=false;entered=false;sig();rad()};sig();perf();rad();lic();clk();setInterval(sig,5000);setInterval(perf,5000);setInterval(rad,90000);setInterval(resultCheck,3000);setInterval(clk,1000);setInterval(cd,250)</script></body></html>''')

if __name__=='__main__':
 import uvicorn
 uvicorn.run(app,host='0.0.0.0',port=int(os.getenv('PORT','8000')))
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const crypto = require('crypto');
const PORT = Number(process.env.PORT || 8080);
const ROOT = __dirname;

const mime = {'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml','.ico':'image/x-icon'};
const httpServer = http.createServer((req,res)=>{
  let u = (req.url || '/').split('?')[0];
  if(u === '/health'){res.writeHead(200,{'Content-Type':'text/plain; charset=utf-8'}); return res.end('ASHFALL OK');}
  if(u === '/') u='/index.html';
  const file = path.join(ROOT, path.normalize(u).replace(/^([.][.][/\\])+/,''));
  if(!file.startsWith(ROOT) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {res.writeHead(404); return res.end('Not found');}
  res.writeHead(200,{'Content-Type':mime[path.extname(file).toLowerCase()]||'application/octet-stream','Cache-Control':'no-cache'});
  fs.createReadStream(file).pipe(res);
});

const wss = new WebSocket.Server({server:httpServer});
const rooms = new Map();
const id = () => crypto.randomBytes(4).toString('hex');
const code = () => { let c; do c=Math.random().toString(36).slice(2,8).toUpperCase(); while(rooms.has(c)); return c; };
function send(ws,msg){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg));}
function broadcast(room,msg,except){const s=JSON.stringify(msg);for(const p of room.players.values())if(p!==except&&p.ws.readyState===WebSocket.OPEN)p.ws.send(s);}
function snapshot(room){return [...room.players.values()].map(p=>({id:p.id,n:p.n,state:p.state||null}));}

wss.on('connection',ws=>{
  const p={ws,id:id(),n:'لاعب',room:null,state:null};
  send(ws,{type:'hello',id:p.id});
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch{return}
    if(m.type==='create'){
      if(p.room)return; const c=code(),room={code:c,players:new Map()}; rooms.set(c,room);
      room.players.set(p.id,p);p.room=room;p.n=String(m.name||'لاعب').slice(0,12);
      send(ws,{type:'room',code:c,id:p.id,players:snapshot(room)}); return;
    }
    if(m.type==='join'){
      if(p.room)return;const c=String(m.code||'').toUpperCase(),room=rooms.get(c);
      if(!room)return send(ws,{type:'error',message:'الغرفة غير موجودة'});
      if(room.players.size>=8)return send(ws,{type:'error',message:'الغرفة ممتلئة'});
      room.players.set(p.id,p);p.room=room;p.n=String(m.name||'لاعب').slice(0,12);
      send(ws,{type:'room',code:c,id:p.id,players:snapshot(room)});
      broadcast(room,{type:'joined',player:{id:p.id,n:p.n}},p);return;
    }
    if(!p.room)return;
    if(m.type==='state'){p.state=m.state||null;broadcast(p.room,{type:'state',id:p.id,state:p.state,n:p.n},p);}
    else if(m.type==='hit'){const target=p.room.players.get(String(m.target));if(target)send(target.ws,{type:'hit',from:p.id,fromName:p.n,damage:Number(m.damage)||0});}
  });
  ws.on('close',()=>{if(!p.room)return;const room=p.room;room.players.delete(p.id);broadcast(room,{type:'left',id:p.id});if(!room.players.size)rooms.delete(room.code);p.room=null;});
});

httpServer.listen(PORT,'0.0.0.0',()=>console.log(`ASHFALL listening on ${PORT}`));

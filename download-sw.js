// Only intercept unguessable, short-lived download routes created by this site.
const activeDownloads=new Map();
const root=new URL('./',self.location.href).pathname;
self.addEventListener('install',(event)=>event.waitUntil(self.skipWaiting()));
self.addEventListener('activate',(event)=>event.waitUntil(self.clients.claim()));
self.addEventListener('message',(event)=>{
  const data=event.data,port=event.ports[0];
  if(!data||data.type!=='enlace-download'||!port||!event.source||
     !/^[a-f0-9]{48}$/.test(data.id)||typeof data.name!=='string'||data.name.length>255||
     !Number.isSafeInteger(data.size)||data.size<1||data.size>899999680||activeDownloads.has(data.id))return;
  let controller,waiting=null,bytes=0,ended=false,finished;
  const lifetime=new Promise((resolve)=>{finished=resolve;});
  event.waitUntil(lifetime);
  const cleanup=()=>{activeDownloads.delete(data.id);clearTimeout(deadline);clearInterval(clientWatch);finished();};
  const fail=()=>{
    if(ended)return;ended=true;
    try{controller.error(new Error('Download interrupted'));}catch{}
    try{port.postMessage({type:'error'});}catch{}
    cleanup();port.close();
  };
  // A large transfer on a slow connection can take hours; this only bounds
  // abandoned sessions, and is refreshed whenever a chunk arrives.
  let deadline=setTimeout(fail,21600000);
  const clientWatch=setInterval(()=>{self.clients.get(event.source.id).then((client)=>{if(!client)fail();},fail);},30000);
  const stream=new ReadableStream({
    start(value){controller=value;},
    pull(){if(waiting!==null){port.postMessage({type:'ack',sequence:waiting});waiting=null;}},
    cancel(){fail();}
  });
  port.onmessage=({data:message})=>{
    if(!message||ended)return;
    clearTimeout(deadline);deadline=setTimeout(fail,21600000);
    if(message.type==='abort'){fail();return;}
    if(!Number.isSafeInteger(message.sequence)||waiting!==null){fail();return;}
    if(message.type==='chunk'){
      if(!(message.buffer instanceof ArrayBuffer)||!message.buffer.byteLength||bytes+message.buffer.byteLength>data.size){fail();return;}
      bytes+=message.buffer.byteLength;
      try{controller.enqueue(new Uint8Array(message.buffer));}catch{fail();return;}
      if(controller.desiredSize>0)port.postMessage({type:'ack',sequence:message.sequence});
      else waiting=message.sequence;
    }else if(message.type==='end'){
      if(bytes!==data.size){fail();return;}
      ended=true;controller.close();port.postMessage({type:'ack',sequence:message.sequence});cleanup();port.close();
    }else fail();
  };
  activeDownloads.set(data.id,{stream,port,name:data.name,size:data.size});
  port.postMessage({type:'ready'});
});
self.addEventListener('fetch',(event)=>{
  const url=new URL(event.request.url),prefix=root+'__enlace_download__/';
  if(url.origin!==self.location.origin||!url.pathname.startsWith(prefix)||event.request.method!=='GET')return;
  const id=url.pathname.slice(prefix.length),item=activeDownloads.get(id);
  if(!item||url.search||!/^[a-f0-9]{48}$/.test(id))return;
  activeDownloads.delete(id);
  const filename=encodeURIComponent(item.name.replace(/[/\\\x00-\x1f\x7f]/g,'_')||'archivo').replace(/['()*]/g,(c)=>'%'+c.charCodeAt(0).toString(16).toUpperCase());
  event.respondWith(new Response(item.stream,{headers:{
    'Content-Type':'application/octet-stream',
    'Content-Disposition':"attachment; filename=\"archivo\"; filename*=UTF-8''"+filename,
    'Content-Length':String(item.size),'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'
  }}));
  item.port.postMessage({type:'started'});
});

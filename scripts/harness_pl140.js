const vm=require('vm'),fs=require('fs'); const src=fs.readFileSync('/home/claude/pl_new.js','utf8');
const urls=[]; const ctx={UrlFetchApp:{fetch:(u,o)=>{urls.push(u);return {getResponseCode:()=>200,getContentText:()=>JSON.stringify({chart:{result:[{meta:{},events:{dividends:{"1719792000":{amount:0.3,date:1719792000}}}}],error:null}})};}},
  Utilities:{sleep:()=>{},formatDate:()=>'2026-09-01'},Logger:{log:()=>{}},console,Date,String,Number,Math,JSON,Object,Array,RegExp,parseFloat,parseInt,isNaN,Error,encodeURIComponent};
vm.createContext(ctx); vm.runInContext(src,ctx);
if(ctx.plYahooSymbol_('SHG.US')!=='SHG'||ctx.plYahooSymbol_('EPRT.us')!=='EPRT'||ctx.plYahooSymbol_('2222.SR')!=='2222.SR'||ctx.plYahooSymbol_('T82U.SI')!=='T82U.SI'||ctx.plYahooSymbol_('OTIS')!=='OTIS') throw new Error('mapper');
console.log('T1 PASS plYahooSymbol_: SHG.US->SHG, EPRT.us->EPRT, 2222.SR/T82U.SI/OTIS unchanged');
const r=ctx.plFetchDividends_('SHG.US',new Date('2026-08-24'));
if(!r.ok||r.list.length!==1) throw new Error('fetch '+JSON.stringify(r));
if(!/\/v8\/finance\/chart\/SHG\?/.test(urls[0])||/SHG\.US/.test(urls[0])) throw new Error('url '+urls[0]);
ctx.plFetchDividends_('5023.SR',new Date('2026-01-01')); if(!/chart\/5023\.SR\?/.test(urls[urls.length-1])) throw new Error('sr url');
console.log('T2 PASS real plFetchDividends_: URL path is /chart/SHG?… (no .US), .SR passed through; response parsed (1 event)');
console.log('PASS 2/2 — real 21_Portfolio_Ledger v1.4.0 under node vm');

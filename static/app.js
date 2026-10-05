/* SPDX-License-Identifier: AGPL-3.0-only
 * Copyright (C) 2026 Jas-Jas-Yuan-debug
 */
"use strict";

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[char]));
const numeric = value => value === null || value === undefined || value === "" || !Number.isFinite(Number(value)) ? null : Number(value);
const firstNumber = (...values) => values.map(numeric).find(value => value !== null) ?? null;
const sum = values => values.every(value => numeric(value) !== null) ? values.reduce((total, value) => total + Number(value), 0) : null;
const amount = (value, digits = 0) => numeric(value) === null ? "—" : Number(value).toLocaleString("en-US", {minimumFractionDigits:digits, maximumFractionDigits:digits});
const quantity = value => numeric(value) === null ? "—" : Number(value).toLocaleString("en-US", {minimumFractionDigits:0, maximumFractionDigits:8});
const percent = value => numeric(value) === null ? "—" : `${Number(value) > 0 ? "+" : ""}${amount(value, 2)}%`;
const signed = (value, digits = 0) => numeric(value) === null ? "—" : `${Number(value) > 0 ? "+" : ""}${amount(value, digits)}`;
const signClass = value => numeric(value) === null || Number(value) === 0 ? "muted" : Number(value) > 0 ? "positive" : "negative";
const shortAmount = value => {const number = numeric(value); if(number === null)return "—"; const absolute = Math.abs(number); return absolute >= 1e6 ? `${amount(number / 1e6, absolute >= 1e7 ? 1 : 2)}M` : absolute >= 1e3 ? `${amount(number / 1e3, 0)}k` : amount(number, absolute < 10 ? 2 : 0);};
const asArray = value => Array.isArray(value) ? value : [];
const timestamp = item => item?.timestamp ?? item?.createdAt ?? item?.created_at ?? item?.time ?? item?.date ?? null;
function parseDate(value){if(value === null || value === undefined || value === "")return null; const normalized = typeof value === "string" && /^\d{4}-\d\d-\d\d[T ]\d\d:\d\d/.test(value) && !/(Z|[+-]\d\d:\d\d)$/i.test(value) ? `${value.replace(" ","T")}Z` : value; const date = new Date(normalized); return Number.isFinite(date.getTime()) ? date : null;}
const displayTime = value => {const date=parseDate(value);return date ? date.toLocaleTimeString("en-GB",{timeZone:"Asia/Shanghai",hour:"2-digit",minute:"2-digit",second:"2-digit"}) : "—";};
const displayDateTime = value => {const date=parseDate(value);return date ? date.toLocaleString("en-GB",{timeZone:"Asia/Shanghai",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"}) : "Unavailable";};
const displayReplayTime=value=>{const date=parseDate(value);return date?date.toLocaleString("en-GB",{timeZone:"Asia/Shanghai",year:"numeric",month:"short",day:"2-digit",hour:"2-digit",minute:"2-digit"}):"Replay time not assigned";};
const dateMillis = item => parseDate(timestamp(item))?.getTime() ?? 0;
const routeText = {overview:["Overview","The arena, at a glance.","Ten AI investors. Independent strategies. Shared markets."],markets:["Markets","A world of opportunities.","Explore A-shares, NASDAQ, NYSE, Bitcoin, and Ethereum."],conversations:["Conversations","Where ideas meet influence.","A shared conversation floor for all ten investors."],activity:["Activity","Every move, in the open.","Follow the research and reasoning behind the arena’s decisions."]};
let currentWorld=new URLSearchParams(location.search).get("world")==="historical"?"historical":"main",worldEpoch=0,worldsMetadata=null,wallClockOffset=0,selectedHistoricalBranchId=null;
const requestControllers=new Set();
let state=null, route="overview", busyAction=false, refreshing=false, chartMode="equity", messageKind="message", catalogOffset=0, catalogTotal=0, catalogItems=[], selectedInstrument=null, catalogRequest=0, quoteRequest=0, agentRequest=0, searchTimer=null, selectedAgentId=null, catalogLoaded=false;
const pageSize=25;
const instrumentCache=new Map(), agentDetailRequests=new Map(), agentDetailCache=new Map();
let agentDetailSignature="", simulationEpoch=0;
const eventMemory=new Map(), seenRoomIds=new Set();
let stateRefreshPromise=null, streamSource=null, streamStatus="connecting", streamReconnectTimer=null, streamVersion=0, latestStreamSnapshot=null, latestStreamRevision=null, latestServerInstance=null, fullRefreshTimer=null, fullRefreshPending=false, lastFullRefreshAt=0, streamMeaningfulSignature="", chatScope="all", roomInitialized=false, pendingChatRoomId=null;

const WORLD_SWITCH_MESSAGE="World selection changed.";
function worldChangedError(){const error=new Error(WORLD_SWITCH_MESSAGE);error.name="WorldSwitchAbort";return error;}
function worldApiPath(path,world=currentWorld){
  if(world!=="historical")return path;
  if(/^\/api\/(state(?:\?|$)|instruments(?:\?|$)|quote\/|history\/|agents\/|messages(?:\?|$)|chat(?:\/|\?|$))/.test(path))return path.replace("/api/","/api/worlds/historical/");
  return path;
}
function worldMatchesPayload(payload,world=currentWorld){
  if(payload?.worldId)return payload.worldId===world;
  if(world==="main")return payload?.isHistorical!==true&&payload?.runtime?.isHistorical!==true;
  return payload?.isHistorical===true||payload?.runtime?.isHistorical===true;
}
function worldBaseline(snapshot=state,world=currentWorld){
  const rows=asArray(snapshot?.agents),historical=(snapshot?.worlds??worldsMetadata)?.historical;
  return firstNumber(snapshot?.totals?.initialCapital,snapshot?.totals?.initialCapitalCny,world==="historical"?historical?.baselineCapital:null,rows.length?sum(rows.map(agent=>firstNumber(agent.initialCash,world==="main"?800000:null))):null);
}
async function api(path, options={}){
  const controller=new AbortController(),epoch=worldEpoch,world=currentWorld;
  if(world==="historical"&&/^\/api\/(simulation|settings)(?:\/|$)/.test(path))throw new Error("Historical world controls are managed automatically.");
  requestControllers.add(controller);const timeout=setTimeout(()=>controller.abort(),options.timeout??30000);
  try{
    const response=await fetch(worldApiPath(path,world),{...options,headers:{"Content-Type":"application/json",...(options.headers??{})},signal:controller.signal});
    let body;try{body=await response.json();}catch{body=null;}
    if(epoch!==worldEpoch)throw worldChangedError();
    if(!response.ok){const detail=body?.detail??body?.error??body?.message;throw new Error(typeof detail==="string"?detail:detail?JSON.stringify(detail):`Request failed (${response.status})`);}
    return body;
  }catch(error){if(epoch!==worldEpoch)throw worldChangedError();if(error.name==="AbortError")throw new Error("The arena did not respond in time. Please try again.");throw error;}
  finally{clearTimeout(timeout);requestControllers.delete(controller);}
}
function clearWorldNamespaces(){
  catalogRequest++;quoteRequest++;agentRequest++;simulationEpoch++;catalogLoaded=false;catalogItems=[];catalogOffset=0;catalogTotal=0;selectedInstrument=null;selectedAgentId=null;agentDetailSignature="";
  instrumentCache.clear();agentDetailCache.clear();agentDetailRequests.clear();eventMemory.clear();seenRoomIds.clear();roomInitialized=false;pendingChatRoomId=null;
  $$("dialog").forEach(dialog=>{if(dialog.open)dialog.close();});
  for(const selector of ["#conversation-feed","#activity-feed","#response-feed"]) {const node=$(selector);node.dataset.rendered="";node.innerHTML=emptyState("Loading this world","Portfolios, responses, and messages are separate for each world.");}
  $("#agent-table-body").innerHTML='<tr><td colspan="8" class="table-loading">Loading investors for this world…</td></tr>';$("#investor-progress").innerHTML="";$("#chat-investors").innerHTML="";$("#message-count").textContent="0";
  $("#investor-cash-grid").innerHTML='<p class="cash-loading">Loading this world’s cash balances…</p>';delete $("#investor-cash-grid").dataset.rendered;$("#cash-total").textContent="Awaiting all ten balances";$("#cash-updated").textContent="Updating live";
  $("#equity-chart").innerHTML=emptyState("Loading portfolio history","This chart belongs only to the selected world.");$("#instrument-detail").innerHTML=emptyState("Select a market instrument","Quotes and price history are isolated to this world.","↗");delete $("#instrument-detail").dataset.instrumentId;
  $("#instruments-body").innerHTML='<tr><td colspan="4" class="table-loading">Loading this world’s instrument catalog…</td></tr>';$("#catalog-total").textContent="— instruments";$("#catalog-coverage").textContent="Checking this world’s market coverage…";$("#pagination-label").textContent="—";$("#pagination-page").textContent="—";$("#catalog-prev").disabled=true;$("#catalog-next").disabled=true;
  $("#message-content").value="";$("#instrument-search").value="";$("#market-filter").value="";$("#message-recipient").innerHTML='<option value="all">Everyone · public</option>';$("#chat-filter").innerHTML='<option value="">All investors / groups</option>';delete $("#message-recipient").dataset.signature;delete $("#chat-filter").dataset.signature;
  $("#featured-quotes").innerHTML="";$("#market-sessions").innerHTML='<div class="loading-line">Loading this world’s market sessions…</div>';$("#snapshot-note").textContent="Loading recorded valuations";
}
function resetSelectedWorldData(){
  worldEpoch++;for(const controller of requestControllers)controller.abort();requestControllers.clear();
  if(streamSource)streamSource.close();streamSource=null;clearTimeout(streamReconnectTimer);streamReconnectTimer=null;clearTimeout(fullRefreshTimer);fullRefreshTimer=null;clearTimeout(searchTimer);fullRefreshPending=false;stateRefreshPromise=null;refreshing=false;
  latestStreamSnapshot=null;latestStreamRevision=null;latestServerInstance=null;streamMeaningfulSignature="";streamVersion=0;streamStatus="connecting";lastFullRefreshAt=0;state=null;busyAction=false;
  clearWorldNamespaces();$("#load-error").hidden=true;
}
function prepareSnapshotBranch(payload){
  if(currentWorld!=="historical")return false;
  const branch=payload?.branchId;if(typeof branch!=="string"||!branch)return false;
  const previous=selectedHistoricalBranchId??state?.branchId??latestStreamSnapshot?.branchId;selectedHistoricalBranchId=branch;
  if(!previous||previous===branch)return false;
  // A new replay branch has new wallets, dates, and history. A process restart
  // that restores the same branch keeps its UI and recorded response caches.
  resetSelectedWorldData();worldsMetadata=payload.worlds??null;wallClockOffset=0;return true;
}
function switchWorld(world){
  if(!["main","historical"].includes(world)||world===currentWorld)return;
  currentWorld=world;selectedHistoricalBranchId=null;resetSelectedWorldData();renderWorlds();renderControls();renderMetrics();renderConnection();changeRoute();
  const url=new URL(location.href);if(world==="historical")url.searchParams.set("world",world);else url.searchParams.delete("world");history.replaceState(null,"",url.pathname+url.search+url.hash);
  refreshState(true);connectStream();
}
function historicalPhaseLabel(phase){return {preparing:"Preparing historical prices",active:"Historical trading active",merge_window:"Closing · model choice window",closed:"Historical world closed",unavailable:"Historical prices unavailable",paused:"Paused by main control"}[phase]??(phase?String(phase).replaceAll("_"," "):"Awaiting world controller");}
function renderWorlds(){
  if(state?.worlds)worldsMetadata=state.worlds;const worlds=worldsMetadata??{},historical=worlds.historical??{},main=worlds.main??{},isHistorical=currentWorld==="historical";
  if(parseDate(state?.serverTime))wallClockOffset=parseDate(state.serverTime).getTime()-Date.now();
  $$("[data-world]").forEach(button=>{const selected=button.dataset.world===currentWorld;button.classList.toggle("selected",selected);button.setAttribute("aria-pressed",String(selected));});document.body.classList.toggle("historical-world",isHistorical);
  $("#workspace-world-name").innerHTML=isHistorical?'Historical world<small>2023 stock replay</small>':'Main world<small>Live investment lab</small>';
  $("#starting-capital-note").textContent=isHistorical?"Seed: copied main cash · no debit":"Starting balance: 800,000 / agent";
  $("#world-clock-label").textContent=isHistorical?"2023 HISTORICAL MARKET CLOCK":"LIVE MARKET CLOCK";$("#selected-world-title").textContent=isHistorical?(historical.name??"Historical stock world"):(main.name??"Main world");
  $("#world-phase").textContent=isHistorical?historicalPhaseLabel(historical.phase):typeof main.stockMarketsOpen==="boolean"?main.stockMarketsOpen?"A stock venue is open":"All stock venues closed":"Checking main exchanges";
  $("#world-phase").className=`world-phase ${isHistorical?historical.phase??"unknown":main.stockMarketsOpen?"active":""}`;
  $("#world-controller-status").textContent=worldsMetadata?`Historical: ${historicalPhaseLabel(historical.phase)}`:"Awaiting world controller";
  $("#world-description").textContent=isHistorical?"Separate wallets, genuine 2023 session OPEN stock prices, and dated crypto bars when available. Initial cash is copied from Main without debiting Main.":"Live quotes, main wallets, and independent investor cycles. Historical results can affect cash only after each model chooses.";
  $("#world-facts").innerHTML=isHistorical?`<div><span>Replay time · Shanghai</span><strong>${esc(displayReplayTime(historical.replayTime))}</strong></div><div><span>Cloned cash baseline</span><strong>${amount(firstNumber(historical.baselineCapital,worldBaseline()))} <small>VC</small></strong></div><div><span>Historical activation · wall clock</span><strong>${historical.startedAt?esc(displayDateTime(historical.startedAt)):"Not started"}</strong></div>`:`<div><span>Price clock</span><strong>Live exchange timestamps</strong></div><div><span>World funding</span><strong>${amount(worldBaseline())} <small>VC initial capital</small></strong></div><div><span>Historical activation rule</span><strong>Only while all main stock venues are closed</strong></div>`;
  $("#world-clock-note").hidden=!isHistorical;$("#world-clock-note").textContent="The market date advances to the next OPEN session every five wall minutes. Investors keep their independent continuous cycles. Price timestamps use replay time; responses, knowledge updates, and merge deadlines use wall time. Stock prices come from actual daily OPEN bars; crypto bar types are labelled from the returned data.";
  $("#leaderboard-description").textContent=isHistorical?"Historical wallets were seeded from Main’s available cash without a debit. Returns use each investor’s actual cloned starting cash.":"Each investor starts with 800,000 VC. Select an investor to explore their portfolio.";
  $("#world-footer-label").textContent=`${isHistorical?"Historical 2023 stock world":"Main live world"} · virtual trading only · 1 VC = 1 CNY`;
  $("#session-clock-tag").innerHTML=isHistorical?'<span class="status-dot pending"></span>2023 OPEN':'<span class="status-dot green"></span>LIVE';
  $("#session-policy").textContent=isHistorical?"Replay time governs historical sessions. Stock OPEN and crypto bar types are labelled; unavailable prices remain unavailable.":"Stocks follow exchange sessions. Crypto trades 24/7.";
  const merge=worlds.merge??{},decisions=asArray(merge.decisions);$("#world-merge-panel").hidden=!(decisions.length||historical.phase==="merge_window");
  const mergeNode=$("#merge-decisions"),openReceipts=new Set($$("details[open]",mergeNode).map(node=>node.dataset.receiptFor));const mergeHTML=decisions.length?decisions.map(decision=>{const status=String(decision.status??"pending"),label={pending:"Awaiting model choice",merged:"Net result transferred",declined:"Transfer declined",failed:"Decision failed",expired:"Choice window expired"}[status]??status;const receipt=decision.receipt;return `<article class="merge-decision"><div class="merge-decision-top"><strong>${esc(agentName(decision.agentId))}</strong><span class="merge-status ${esc(status)}">${esc(label)}</span><span class="merge-delta ${signClass(decision.deltaVC)}">${signed(decision.deltaVC,2)} <small>VC net result</small></span></div>${decision.explanation?`<p class="public-explanation">${publicTextHTML(decision.explanation)}</p>`:`<p class="merge-explanation-missing">${status==="pending"?"The model has not supplied its transfer choice yet.":"No public transfer explanation was supplied."}</p>`}${receipt?`<details class="transfer-receipt" data-receipt-for="${esc(decision.agentId)}"><summary>Actual transfer receipt</summary><pre>${esc(typeof receipt==="string"?receipt:JSON.stringify(receipt,null,2))}</pre></details>`:""}</article>`;}).join(""):emptyState("Awaiting individual transfer choices","The five-minute choice window opens when a main stock venue opens.");if(mergeNode.dataset.rendered!==mergeHTML){mergeNode.innerHTML=mergeHTML;mergeNode.dataset.rendered=mergeHTML;$$("details",mergeNode).forEach(node=>{node.open=openReceipts.has(node.dataset.receiptFor);});}
  $("#merge-deadline-note").textContent=merge.deadlineAt?`Opened ${displayDateTime(merge.openedAt)} · Deadline ${displayDateTime(merge.deadlineAt)} · Shanghai wall clock. Only net VC profit/loss transfers; main positions stay unchanged.`:"Each investor receives its own model decision; there is no automatic profit/loss transfer.";
  updateMergeCountdown();
}
function updateMergeCountdown(){const deadline=(state?.worlds??worldsMetadata)?.merge?.deadlineAt,date=parseDate(deadline),node=$("#merge-countdown");if(!node)return;if(!date){node.textContent="Deadline not reported";return;}const remaining=Math.max(0,Math.ceil((date.getTime()-Date.now()-wallClockOffset)/1000));node.textContent=remaining?`${String(Math.floor(remaining/60)).padStart(2,"0")}:${String(remaining%60).padStart(2,"0")}`:"Window ended";}
function toast(message,type="success"){
  if(message===WORLD_SWITCH_MESSAGE)return;
  const node=document.createElement("div");node.className=`toast ${type==="error"?"error":""}`;node.innerHTML=`<span>${esc(message)}</span><button aria-label="Dismiss notification">×</button>`;
  $("button",node).addEventListener("click",()=>node.remove());$("#toast-region").append(node);setTimeout(()=>node.remove(),type==="error"?10000:6000);
}
function emptyState(title,description,symbol="⌁"){return `<div class="empty-state"><span class="empty-symbol" aria-hidden="true">${esc(symbol)}</span><strong>${esc(title)}</strong><p>${esc(description)}</p></div>`;}
const agents = () => asArray(state?.agents);
const agentById = id => agents().find(agent=>agent.id===id);
const agentName = id => id==="human" ? "You" : id==="arena" ? "Arena" : id==="all" ? "Everyone" : agentById(id)?.name ?? id ?? "Arena";
const isSol = agent => String(agent?.model??agent?.id??"").includes("sol");
const agentInitials = agent => `${isSol(agent)?"S":"A"}${String(agent?.id??"").match(/\d+$/)?.[0]??""}`;
function avatar(agent, human=false){return `<span class="avatar ${human?"human":isSol(agent)?"sol":""}">${human?"YOU":esc(agentInitials(agent))}</span>`;}
function ingestEvents(events){
  for(const event of asArray(events)){const key=event.id!==undefined?`id:${event.id}`:`${event.agentId}:${event.kind}:${timestamp(event)}:${event.content}`;eventMemory.set(key,event);}
  if(eventMemory.size>2000){const entries=[...eventMemory.entries()].sort((a,b)=>dateMillis(a[1])-dateMillis(b[1]));for(const [key]of entries.slice(0,eventMemory.size-2000))eventMemory.delete(key);}
}
function agentEvents(id){return [...eventMemory.values()].filter(event=>(event.agentId??event.agent_id)===id).sort((a,b)=>dateMillis(b)-dateMillis(a)||Number(b.id??0)-Number(a.id??0));}
function metadataFor(agent){return {...(state?.runtime?.agentStates?.[agent?.id]??{}),...(agent?.decisionMetadata??{}),...agent};}
function latestDecisionFor(agent){
  const meta=metadataFor(agent),direct=agent?.latestDecision??agent?.lastDecision??agent?.decision??meta.latestDecision??meta.lastDecision;
  if(direct&&typeof direct==="object")return direct;
  if(typeof direct==="string")return {kind:"decision",content:direct,timestamp:meta.lastDecisionAt,details:{orders:meta.lastDecisionOrders}};
  return agentEvents(agent?.id).find(event=>event.kind==="decision")??null;
}
function lifecycleFor(agent){
  if(currentWorld==="main"&&state?.runtime?.remoteConnected===false)return {label:"Main status unavailable",tone:"failed",raw:"remote_unavailable",description:`Current main runtime status cannot be verified. ${state.runtime.remoteError??""}`};
  const meta=metadataFor(agent),raw=String(agent?.lifecycleStatus??agent?.phase??state?.runtime?.agentStates?.[agent?.id]?.status??agent?.status??"idle").toLowerCase();
  const labels={researching:"Retrieving evidence",retrieving_evidence:"Retrieving evidence",waiting_for_model:"Waiting for model slot",generating_response:"Awaiting model response",waiting_for_retry:"Waiting for retry",response_completed:"Response received",executing_tools:"Executing requested tools",updating_knowledge:"Updating knowledge",knowledge_updated:"Knowledge refreshed",thinking:"Evaluating evidence",evaluating_evidence:"Evaluating evidence",running:"Evaluating evidence",busy:"Evaluating evidence",processing:"Evaluating evidence",trading:"Submitting orders"};
  if(["error","failed","unavailable"].includes(raw))return {label:"Decision failed",tone:"failed",raw,description:agent?.error??meta.error??"The latest evaluation did not produce a completed decision."};
  if(labels[raw])return {label:labels[raw],tone:"busy",raw,description:labels[raw]};
  const decision=latestDecisionFor(agent),orders=firstNumber(decision?.details?.orders,decision?.details?.orderCount,decision?.orders,meta.lastDecisionOrders);
  if(decision&&orders===0&&!Array.isArray(agent?.positions))return {label:"Decision recorded",tone:"",raw,description:"The decision executed no orders; the current holdings are unavailable."};
  if(decision&&orders!==null){const positions=asArray(agent?.positions);return {label:orders>0?"Orders executed":positions.some(position=>numeric(position.quantity)>0)?"Maintaining positions":"Holding cash",tone:"completed",raw,description:`Latest completed decision: ${amount(orders)} executed order${orders===1?"":"s"}; ${displayDateTime(timestamp(decision))} Shanghai`};}
  if(decision)return {label:"Decision recorded",tone:"",raw,description:"A public decision is recorded; its executed order count is unavailable."};
  return {label:raw==="paused"?"Awaiting next evaluation":"Awaiting first decision",tone:"",raw,description:"No completed investment decision is recorded."};
}
function agentStatus(agent){const status=lifecycleFor(agent);return `<span class="agent-status ${status.tone}" title="${esc(status.description)}"><span class="status-dot ${status.tone==="failed"?"red":status.tone==="busy"?"pending":status.tone==="completed"?"green":""}"></span>${esc(status.label)}</span>`;}
function responseMetadata(agent){
  const meta=metadataFor(agent),events=agentEvents(agent?.id),response=events.find(event=>event.kind==="response_completed"),knowledge=events.find(event=>event.kind==="knowledge_updated");
  return {responseCount:firstNumber(meta.responseCount),lastResponseAt:meta.lastResponseAt??timestamp(response),knowledgeUpdateCount:firstNumber(meta.knowledgeUpdateCount),knowledgeUpdatedAt:meta.knowledgeUpdatedAt??timestamp(knowledge),cycle:firstNumber(meta.cycle),completedCycles:firstNumber(meta.completedCycles),lastExplanation:meta.lastExplanation,lastResponseKind:meta.lastResponseKind};
}
function updateMetadataHTML(agent){const meta=responseMetadata(agent);return `<span class="agent-update-info">${meta.cycle!==null?`Cycle ${amount(meta.cycle)} · `:""}${meta.responseCount===null?"Response count unavailable":`${amount(meta.responseCount)} response${meta.responseCount===1?"":"s"}`} · ${meta.lastResponseAt?displayTime(meta.lastResponseAt):"none recorded"}</span><span class="agent-update-info">Knowledge ${meta.knowledgeUpdatedAt?displayTime(meta.knowledgeUpdatedAt):"not recorded"}${meta.knowledgeUpdateCount!==null?` · ${amount(meta.knowledgeUpdateCount)} updates`:""}</span>`;}
function riskBadge(agent){const profile=agent?.riskProfile;return profile?.style?`<span class="risk-badge ${esc(profile.style)}" title="${esc(profile.description??profile.style)}">${esc(profile.style)}</span>`:"";}
function publicTextHTML(value){const text=String(value??"");let html="",last=0;for(const match of text.matchAll(/https?:\/\/[^\s<>"']+/g)){html+=esc(text.slice(last,match.index));const trimmed=match[0].replace(/[),.;\]]+$/,"");const url=safeWebLink(trimmed);html+=url?`<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(trimmed)}</a>${esc(match[0].slice(trimmed.length))}`:esc(match[0]);last=match.index+match[0].length;}return html+esc(text.slice(last));}
function responseText(event){return typeof event?.details?.explanation==="string"?event.details.explanation:event?.kind==="decision"?event.content??"":event?.details?.explanation===null?"":event?.content??"";}
function responseSources(event){const explicit=asArray(event?.details?.sources??event?.details?.evidence);if(explicit.length)return {label:"Evidence provided in the response",items:explicit};const research=agentEvents(event?.agentId??event?.agent_id).find(item=>item.kind==="research"&&dateMillis(item)<=dateMillis(event));return {label:"Research available to this investor",items:asArray(research?.details?.results??research?.details?.sources)};}
function responseCardHTML(event,compact=false){
  const id=event.agentId??event.agent_id,agent=agentById(id),details=event.details??{},text=responseText(event),tools=asArray(details.toolActions),final=event.kind==="decision"||details.kind==="final",toolOnly=!text.trim()&&tools.length>0;
  const label=event.kind==="decision"?"Final decision":toolOnly?"Tool requests only":final?"Final public response":"Intermediate public response";
  const matchingDecision=final?latestDecisionFor(agent):null;const orders=firstNumber(details.orders,details.orderCount,(matchingDecision?.details?.roundId??matchingDecision?.roundId)===details.roundId?firstNumber(matchingDecision?.details?.orders,matchingDecision?.orders):null);const sources=responseSources(event);
  const sourceHTML=sources.items.slice(0,compact?2:4).map(source=>{const url=safeWebLink(typeof source==="string"?source:source.url??source.link??source.href);return url?`<a class="research-link" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(typeof source==="string"?source:source.title??source.name??url)} ↗</a>`:"";}).filter(Boolean).join("");
  const lists=[['Risks',details.risks],['Assumptions',details.assumptions]].map(([label,items])=>{const list=Array.isArray(items)?items:typeof items==="string"?[items]:[];return list.length?`<div class="response-context"><strong>${label}</strong><p>${list.map(item=>esc(typeof item==="string"?item:JSON.stringify(item))).join("<br>")}</p></div>`:"";}).join("");
  return `<article class="response-card ${compact?"compact":""}"><div class="response-card-header">${avatar(agent)}<div><strong>${esc(agentName(id))}</strong><span class="response-type ${toolOnly?"tool-only":""}">${label}${details.responseNumber!==undefined?` · #${amount(details.responseNumber)}`:""}</span></div><time title="${esc(displayDateTime(timestamp(event)))} · Shanghai">${displayTime(timestamp(event))}</time></div>${text.trim()?`<p class="public-explanation">${publicTextHTML(text)}</p>`:`<p class="response-unavailable">${toolOnly?"This response requested tools and supplied no public explanation.":"No public explanation was supplied in this response."}</p>`}${tools.length?`<div class="requested-tools"><strong>Requested tools</strong>${tools.map(tool=>`<span title="${esc(tool.reason??tool.query??tool.name)}">${esc(tool.name??"Tool")}${tool.instrumentId?` · ${esc(tool.action??"")} ${esc(tool.instrumentId)}${numeric(tool.quantity)!==null?` × ${quantity(tool.quantity)}`:""}`:tool.query?` · ${esc(String(tool.query).slice(0,90))}`:tool.recipient?` · ${esc(agentName(tool.recipient))}`:""}</span>`).join("")}</div>`:""}${orders!==null?`<div class="response-outcome">${amount(orders)} executed order${orders===1?"":"s"} in this completed decision</div>`:""}${lists}${sourceHTML?`<div class="response-evidence"><strong>${sources.label}</strong>${sourceHTML}</div>`:""}</article>`;
}
function publicResponses(id=null){const list=[...eventMemory.values()].filter(event=>(!id||(event.agentId??event.agent_id)===id)&&["response_completed","decision"].includes(event.kind));
  // Recorded per-investor metadata survives the global activity window; its text is still real API output.
  for(const agent of agents().filter(agent=>!id||agent.id===id)){const decision=latestDecisionFor(agent),meta=responseMetadata(agent);if(decision?.content&&!list.some(event=>(event.agentId??event.agent_id)===agent.id&&event.kind==="decision"&&event.content===decision.content&&timestamp(event)===timestamp(decision)))list.push({...decision,agentId:agent.id,kind:"decision",details:decision.details??{roundId:decision.roundId,orders:decision.orders,researches:decision.researches}});if(meta.lastResponseAt&&!list.some(event=>(event.agentId??event.agent_id)===agent.id&&event.kind==="response_completed"&&timestamp(event)===meta.lastResponseAt))list.push({agentId:agent.id,kind:"response_completed",timestamp:meta.lastResponseAt,details:{kind:meta.lastResponseKind,explanation:meta.lastExplanation??null,toolActions:[]}});}
return list.filter(event=>event.kind!=="decision"||!list.some(response=>response.kind==="response_completed"&&response.details?.kind==="final"&&(response.agentId??response.agent_id)===(event.agentId??event.agent_id)&&response.details?.roundId===event.details?.roundId&&responseText(response)===responseText(event))).sort((a,b)=>dateMillis(b)-dateMillis(a)||Number(b.id??0)-Number(a.id??0));}
function renderResponses(){
  const grid=$("#investor-progress");grid.innerHTML=agents().map(agent=>{const meta=responseMetadata(agent);return `<button class="investor-progress-card" data-agent="${esc(agent.id)}"><span class="progress-name">${avatar(agent)}<strong>${esc(agent.name??agent.id)}</strong>${riskBadge(agent)}</span>${agentStatus(agent)}${updateMetadataHTML(agent)}</button>`;}).join("");
  const feed=$("#response-feed"),responses=publicResponses().slice(0,6),html=responses.length?responses.map(event=>responseCardHTML(event,true)).join(""):emptyState("No public response recorded yet","Model outputs and tool requests will appear here when they are actually received.","↗");if(feed.dataset.rendered!==html){const top=feed.scrollTop;feed.innerHTML=html;feed.dataset.rendered=html;feed.scrollTop=top;}
}
function publicAgentSection(agent){
  const meta=responseMetadata(agent),events=agentEvents(agent.id),response=publicResponses(agent.id)[0],knowledge=events.find(event=>event.kind==="knowledge_updated");
  let publicNote=response?responseCardHTML(response):meta.lastExplanation?`<p class="public-explanation">${publicTextHTML(meta.lastExplanation)}</p>`:emptyState("No public explanation recorded","This investor has not supplied a published decision or response note yet.");
  const profile=agent.riskProfile;const risk=profile?`<div class="risk-guidelines">${riskBadge(agent)}<p>${esc(profile.description??"")}</p><span>Position sizing guideline: ${amount(profile.positionSizingPct)}% · Cash reserve guideline: ${amount(profile.cashReservePct)}%. These are strategy guidelines, not enforced limits.</span></div>`:"";
  const kd=knowledge?.details??{},quotes=asArray(kd.quotes),messages=asArray(kd.newMessages),research=asArray(kd.sharedResearch);
  const known=knowledge?`<div class="knowledge-summary"><strong>Latest refreshed knowledge</strong><span>${amount(quotes.length)} observed quotes · ${amount(messages.length)} new messages${research.length?` · ${amount(research.length)} shared research items`:""}</span>${asArray(kd.errors).length?`<p>${kd.errors.map(error=>esc(typeof error==="string"?error:JSON.stringify(error))).join("<br>")}</p>`:""}</div>`:"";
  return `<div class="detail-section public-decision-section"><h3>Current response &amp; decision</h3><div class="response-stat-row"><span>Response count <strong>${amount(meta.responseCount)}</strong></span><span>Latest response <strong>${meta.lastResponseAt?displayTime(meta.lastResponseAt):"Not recorded"}</strong></span><span>Knowledge updated <strong>${meta.knowledgeUpdatedAt?displayTime(meta.knowledgeUpdatedAt):"Not recorded"}</strong></span><span>Knowledge updates <strong>${amount(meta.knowledgeUpdateCount)}</strong></span>${meta.cycle!==null?`<span>Personal cycle <strong>${amount(meta.cycle)}</strong></span>`:""}</div>${risk}${publicNote}${known}</div>`;
}
function changeRoute(){
  route=location.hash.slice(1);if(!Object.hasOwn(routeText,route))route="overview";
  const [label,title,description]=routeText[route];$("#breadcrumb-current").textContent=label;$("#page-title").textContent=title;$("#page-description").textContent=description;
  $$(".view").forEach(view=>view.hidden=view.id!==`view-${route}`);$$(".nav-item[data-route]").forEach(link=>{link.classList.toggle("active",link.dataset.route===route);if(link.dataset.route===route)link.setAttribute("aria-current","page");else link.removeAttribute("aria-current");});
  if(route==="markets"&&!catalogLoaded)loadCatalog();
  if(route==="markets"&&selectedInstrument)refreshSelectedInstrument();
  if(state){if(route==="conversations")renderMessages();if(route==="activity")renderActivity();}
}
function updateClock(){$("#local-clock").textContent=`Shanghai · ${new Date().toLocaleTimeString("en-GB",{timeZone:"Asia/Shanghai",hour:"2-digit",minute:"2-digit"})}`;}

function streamOverlay(base,payload){const merged={...(base??{})};for(const key of ["runtime","agents","events","messages","trades","serverTime","serverInstance","revision","totals","backgroundService","worldId","branchId","worlds","isHistorical","asOf","replayTime","chat","groups","rooms","activeRoom","activeRooms"])if(Object.hasOwn(payload,key))merged[key]=payload[key];if(Array.isArray(payload.agents)){const equities=payload.agents.map(agent=>agent.equity),equity=payload.agents.length===10?sum(equities):null,cash=sum(payload.agents.map(agent=>agent.cash)),initial=worldBaseline(merged);merged.totals={...(merged.totals??{}),initialCapital:initial,equity,cash,pnl:equity===null||initial===null?null:equity-initial,pnlPct:equity===null||!initial?null:(equity/initial-1)*100};}return merged;}
function acceptStreamSnapshot(payload){
  const instance=typeof payload.serverInstance==="string"?payload.serverInstance:null;
  if(instance&&instance!==latestServerInstance){latestServerInstance=instance;latestStreamRevision=null;streamMeaningfulSignature="";}
  const revision=numeric(payload.revision);if(revision!==null&&latestStreamRevision!==null&&revision<latestStreamRevision)return false;
  latestStreamRevision=revision??latestStreamRevision;latestStreamSnapshot=payload;streamVersion++;return true;
}
function reconcileFullSnapshot(full,startedVersion){
  const fullRevision=numeric(full?.revision),fullInstance=typeof full?.serverInstance==="string"?full.serverInstance:null,streamInstance=latestStreamSnapshot?.serverInstance??null;
  const differentInstance=Boolean(fullInstance&&streamInstance&&fullInstance!==streamInstance);
  const newerStream=streamVersion>startedVersion;
  // Revisions are comparable only inside one server process. Preserve a restarted server's stream
  // when an earlier process's in-flight full request finally completes.
  if(latestStreamSnapshot&&((differentInstance&&(newerStream||streamStatus==="live"))||(!differentInstance&&newerStream&&(fullRevision===null||latestStreamRevision===null||latestStreamRevision>fullRevision))))return streamOverlay(full,latestStreamSnapshot);
  // Polling can discover a restart before the next stream connection does.
  if(fullInstance&&fullInstance!==latestServerInstance){latestServerInstance=fullInstance;latestStreamRevision=null;latestStreamSnapshot=null;streamMeaningfulSignature="";}
  return full;
}
function renderConnection(){const connected=streamStatus==="live",polling=streamStatus==="polling";$("#stream-status").innerHTML=`<span class="status-dot ${connected?"green":polling?"pending":""}"></span>${connected?"Live updates":polling?"Polling fallback":"Connecting"}`;$("#stream-status").title=connected?"Responses and lifecycle changes arrive through the local event stream":polling?"The live stream is unavailable; current data refreshes every seven seconds":"Connecting to the local event stream";$("#connection-dot").className=`status-dot ${connected?"green":polling&&state?"pending":""}`;$("#connection-label").textContent=connected?"Live event stream connected":polling&&state?"Polling · every 7 seconds":"Connecting to arena";}
function queueFullRefresh(){fullRefreshPending=true;if(fullRefreshTimer)return;fullRefreshTimer=setTimeout(()=>{fullRefreshTimer=null;if(stateRefreshPromise)return;fullRefreshPending=false;refreshState(true);},Math.max(900,2500-(Date.now()-lastFullRefreshAt)));}
function meaningfulStreamKey(payload){return JSON.stringify([payload.runtime?.running,payload.runtime?.inProgress,payload.runtime?.completedDecisions,payload.worlds?.historical?.phase,payload.worlds?.historical?.replayTime,payload.worlds?.merge,asArray(payload.events).map(event=>event.id??`${event.kind}:${timestamp(event)}`),asArray(payload.trades).map(trade=>trade.id),asArray(payload.messages).map(message=>message.id),asArray(payload.agents).map(agent=>[agent.id,agent.status,agent.responseCount,agent.knowledgeUpdatedAt])]);}
function connectStream(){
  clearTimeout(streamReconnectTimer);if(streamSource)streamSource.close();if(typeof EventSource==="undefined"){streamStatus="polling";renderConnection();return;}
  streamStatus="connecting";renderConnection();const epoch=worldEpoch,world=currentWorld,source=new EventSource(`/api/stream?world=${encodeURIComponent(world)}`);streamSource=source;
  source.onopen=()=>{if(source!==streamSource||epoch!==worldEpoch)return;streamStatus="live";renderConnection();};
  source.addEventListener("update",event=>{if(source!==streamSource||epoch!==worldEpoch)return;try{const payload=JSON.parse(event.data);if(!payload||typeof payload!=="object"||Array.isArray(payload))return;if(!worldMatchesPayload(payload,world)){streamStatus="polling";renderConnection();return;}const branchChanged=prepareSnapshotBranch(payload);if(!acceptStreamSnapshot(payload))return;state=streamOverlay(state,payload);streamStatus="live";$("#load-error").hidden=true;ingestEvents(payload.events);renderState();renderConnection();if(branchChanged){changeRoute();connectStream();}const meaningful=meaningfulStreamKey(payload);if(meaningful!==streamMeaningfulSignature){streamMeaningfulSignature=meaningful;queueFullRefresh();}}catch{streamStatus="polling";renderConnection();}});
  source.addEventListener("world_unavailable",event=>{if(source!==streamSource||epoch!==worldEpoch)return;try{const payload=JSON.parse(event.data);if(!worldMatchesPayload(payload,world))return;const branchChanged=prepareSnapshotBranch({...payload,branchId:payload.branchId??payload.worlds?.historical?.id});if(payload.worlds)worldsMetadata=payload.worlds;streamStatus="polling";renderWorlds();renderControls();renderMetrics();renderConnection();$("#load-error-title").textContent="Historical world data is not available yet";$("#load-error-text").textContent=payload.detail??"Awaiting the world controller";$("#load-error").hidden=false;if(branchChanged){changeRoute();connectStream();}queueFullRefresh();}catch{streamStatus="polling";renderConnection();}});
  source.onerror=()=>{if(source!==streamSource||epoch!==worldEpoch)return;source.close();streamSource=null;streamStatus="polling";renderConnection();refreshState(true);streamReconnectTimer=setTimeout(connectStream,15000);};
}
async function refreshState(silent=false){
  if(stateRefreshPromise)return stateRefreshPromise;refreshing=true;const startedVersion=streamVersion,epoch=worldEpoch;let pending;
  pending=(async()=>{try{const full=await api("/api/state");if(epoch!==worldEpoch)return;if(!worldMatchesPayload(full))throw new Error("The server has not confirmed the selected world’s identity yet.");const branchChanged=prepareSnapshotBranch(full);state=reconcileFullSnapshot(full,branchChanged?streamVersion:startedVersion);lastFullRefreshAt=Date.now();$("#load-error").hidden=true;ingestEvents(state?.events);renderState();renderConnection();if(branchChanged){changeRoute();connectStream();}}
  catch(error){if(epoch!==worldEpoch||error.name==="WorldSwitchAbort")return;$("#connection-dot").className="status-dot red";$("#connection-label").textContent="Selected world unavailable";$("#load-error-title").textContent=currentWorld==="historical"?"Historical world data is not available yet":"Could not reach the arena";$("#load-error-text").textContent=error.message;$("#load-error").hidden=false;renderWorlds();if(!state){$("#runtime-badge").innerHTML='<span class="status-dot pending"></span>Waiting for world data';$$("#run-round, #toggle-simulation").forEach(button=>button.disabled=true);}if(!silent&&state)toast(error.message,"error");}
  finally{if(epoch!==worldEpoch||stateRefreshPromise!==pending)return;refreshing=false;stateRefreshPromise=null;if(fullRefreshPending){fullRefreshPending=false;queueFullRefresh();}}})();stateRefreshPromise=pending;return pending;
}
function renderState(){ingestEvents(state?.events);renderWorlds();renderControls();renderMetrics();renderSessions();renderFeatured();renderAgents();renderEquity();renderRecipients();renderChatRoom();renderMessages();renderResponses();renderActivity();refreshOpenAgent();if(route==="markets"&&selectedInstrument)refreshSelectedInstrument();$("#last-updated").textContent=`Updated ${displayTime(state?.serverTime??new Date().toISOString())} · Shanghai`;}
function renderControls(){
  const runtime=state?.runtime??{}, running=Boolean(runtime.running), inProgress=Boolean(runtime.inProgress??runtime.in_progress),continuous=runtime.mode==="continuous",historical=currentWorld==="historical";
  const provider=state?.provider??{},providerKnown=typeof provider.connected==="boolean";$("#provider-status").textContent=!providerKnown?"CHECKING MODELS":provider.connected?"MODELS CONNECTED":"MODELS OFFLINE";$("#provider-status").title=!providerKnown?"Checking availability of both requested models on the local provider":provider.connected?"The local provider has both requested models available":provider.error??"The local model provider is unavailable";
  const remoteUnavailable=currentWorld==="main"&&runtime.remoteConnected===false;$("#remote-error").hidden=!remoteUnavailable;$("#remote-error-text").textContent=remoteUnavailable?`${runtime.remoteError??"The original main runtime did not answer its status check."} Current lifecycle phases cannot be verified; recorded decisions and portfolios remain visible.`:"";
  $("#runtime-badge").innerHTML=remoteUnavailable?'<span class="status-dot red"></span>Main runtime unavailable':!state?'<span class="status-dot pending"></span>Loading selected world':`<span class="status-dot ${running||inProgress?"green":""}"></span>${continuous?(running?"Continuous cycles active":"Investors paused"):inProgress?"Evaluation in progress":running?"Arena running":"Arena paused"}`;
  $("#toggle-simulation").innerHTML=(running||inProgress)?'<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5h3v14H8zM15 5h3v14h-3z"/></svg><span>Pause arena</span>':'<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m9 5 10 7-10 7Z"/></svg><span>Start arena</span>';
  $("#toggle-simulation").disabled=busyAction||historical||!state||remoteUnavailable;$("#run-round").disabled=busyAction||historical||!state||remoteUnavailable||inProgress||(continuous&&running);$("#run-round").title=continuous&&running?"Independent continuous cycles are already active":inProgress?"An evaluation is already in progress":"Request one evaluation per investor";
  for(const button of $$("#open-settings, #top-settings, #open-reset, #save-settings")){button.disabled=historical;button.title=historical?"Historical world activation and closing are managed automatically":"";}$("#toggle-simulation").title=historical?"Historical trading is managed automatically; switch to Main for global controls":"";
  $("#interval-settings").hidden=continuous||historical;$("#continuous-settings-note").hidden=!continuous;$("#save-settings").hidden=continuous||historical;
  const round=firstNumber(runtime.round,runtime.roundNumber,0);const next=runtime.nextRound??runtime.next_round;
  $("#round-summary").textContent=historical?`Historical world · ${historicalPhaseLabel((state?.worlds??worldsMetadata)?.historical?.phase)} · independent investor cycles`:continuous?`Continuous personal cycles · ${amount(runtime.completedDecisions??0)} completed decisions · ${amount(runtime.activeAgents??0)} active investors`:`Round ${amount(round)}${running&&next?` · Next round ${displayTime(next)}`:runtime.lastRound?` · Last round ${displayTime(runtime.lastRound)}`:" · Waiting for the next decision"}`;
  if(!$("#settings-dialog").open)$("#round-interval").value=runtime.intervalSeconds??300;
  const service=state?.backgroundService;const serviceNode=$("#service-status");if(serviceNode){serviceNode.hidden=!service;serviceNode.textContent=service?`${service.manager==="launchd"?"launchd service":service.manager==="standalone"?"Standalone service":String(service.manager??"Service manager unavailable")}${service.continuesWithoutBrowser?" · continues without browser":""}`:"";}
  updateResetButton();
}
function renderMetrics(){
  const totals=state?.totals??{}, list=agents();
  const totalEquity=firstNumber(totals.equity,totals.totalEquity,list.length?sum(list.map(agent=>agent.equity)):null);
  const initial=worldBaseline();
  const pnl=firstNumber(totals.pnl,totalEquity!==null&&initial!==null?totalEquity-initial:null);
  const pct=firstNumber(totals.pnlPct,initial&&pnl!==null?pnl/initial*100:null);
  const cash=firstNumber(totals.cash,totals.totalCash,list.length?sum(list.map(agent=>agent.cash)):null);
  const trades=firstNumber(totals.tradeCount,totals.totalTrades,Array.isArray(state?.trades)?state.trades.length:null);
  $("#metrics").innerHTML=`<article class="metric-card"><div class="metric-label">${currentWorld==="historical"?"Historical world value":"Main world value"} <span>↗</span></div><div class="metric-value">${amount(totalEquity)}<small>VC</small></div><div class="metric-foot">${totalEquity===null?"Some holdings could not be valued":"Across all ten investors"}</div></article><article class="metric-card"><div class="metric-label">${currentWorld==="historical"?"Historical return":"Main return"} <span>％</span></div><div class="metric-value ${signClass(pnl)}">${signed(pnl)}<small>VC</small></div><div class="metric-foot"><span class="${signClass(pct)}">${percent(pct)}</span><span>${currentWorld==="historical"?"from cloned starting cash":"since the arena began"}</span></div></article><article class="metric-card"><div class="metric-label">Available capital <span>◎</span></div><div class="metric-value">${amount(cash)}<small>VC</small></div><div class="metric-foot">${cash!==null&&initial?`${amount(cash/initial*100,1)}% of starting capital` : "Waiting for account data"}</div></article><article class="metric-card"><div class="metric-label">Completed trades <span>⇄</span></div><div class="metric-value">${amount(trades)}</div><div class="metric-foot">${currentWorld==="historical"?"Real 2023 OPEN prices. Virtual execution.":"Live quotes. Virtual execution."}</div></article>`;
  renderCashBalances();
}
function renderCashBalances(){
  const list=[...agents()].sort((a,b)=>String(a.id).localeCompare(String(b.id))),grid=$("#investor-cash-grid");
  $("#cash-world-label").textContent=currentWorld==="historical"?"Historical 2023":"Main live";
  const html=list.length?list.map(agent=>{const name=agent.name??agent.id,cash=numeric(agent.cash);return `<button type="button" class="cash-card" data-agent="${esc(agent.id)}" aria-label="${esc(name)} available cash: ${cash===null?"unavailable":`${amount(cash,2)} VC`}. View portfolio" title="${cash===null?"Cash balance unavailable":`Exact available cash: ${amount(cash,6)} VC`}"><span class="cash-investor">${avatar(agent)}<strong>${esc(name)}</strong></span><span class="cash-balance">${amount(cash,2)} <small>VC</small></span></button>`;}).join(""):'<p class="cash-loading">Loading this world’s cash balances…</p>';
  if(grid.dataset.rendered!==html){grid.innerHTML=html;grid.dataset.rendered=html;}
  const total=list.length===10?sum(list.map(agent=>agent.cash)):null;
  $("#cash-total").textContent=total===null?"Awaiting all ten balances":`${list.length} investors · ${amount(total,2)} VC total cash`;
  $("#cash-updated").textContent=state?.serverTime?`Updated ${displayTime(state.serverTime)} · Shanghai`:"Updating live";
}
function renderSessions(){
  const raw=state?.markets, list=Array.isArray(raw)?raw:asArray(raw?.markets);
  const sessionDate=currentWorld==="historical"?parseDate((state?.worlds??worldsMetadata)?.historical?.replayTime):new Date();$("#session-date").textContent=sessionDate?sessionDate.toLocaleDateString("en-GB",{timeZone:"Asia/Shanghai",day:"2-digit",month:"long",year:"numeric"}):"Replay date not assigned";
  $("#market-sessions").innerHTML=list.length?list.map(market=>{const open=Boolean(market.isOpen??market.is_open);const id=String(market.id??market.market??"");const badge=/crypto/i.test(id)?"₿":/nasdaq/i.test(id)?"NQ":/nyse/i.test(id)?"NY":/bse/i.test(id)?"BJ":/sz/i.test(id)?"SZ":/sse|sh/i.test(id)?"SH":"CN";return `<div class="session-row"><span class="market-badge">${badge}</span><div class="session-name">${esc(market.name??id)}<small title="${esc(market.reason)}">${esc(market.reason??(open?"Trading is open":"Market closed"))}</small></div><span class="session-status ${open?"":"closed"}" title="${esc(market.nextOpen?`Next open: ${displayDateTime(market.nextOpen)} · Shanghai`:market.reason)}">${open?"Open":"Closed"}</span></div>`;}).join(""):'<div class="loading-line">Market session status unavailable.</div>';
}
function historicalPriceBasis(quote){return {daily_open:"Daily session OPEN",daily_close:"Daily session CLOSE",hourly_close:"Hourly bar CLOSE"}[quote?.barType]??"Price basis not reported";}
function historicalFeaturedRank(raw){const quote=raw.quote??raw,market=quote.market??String(quote.instrumentId??quote.id??"").split(":")[0],stock=quote.kind==="stock"||["SSE","SZSE","BSE","NASDAQ","NYSE"].includes(market);return stock?(quote.validForExecution===true?0:1):2;}
function renderFeatured(){
  let featured=asArray(state?.featured);if(!featured.length)featured=asArray(state?.markets?.featured);
  if(currentWorld==="historical"){
    featured=[...featured].sort((a,b)=>historicalFeaturedRank(a)-historicalFeaturedRank(b));
    $("#featured-quotes").innerHTML=featured.length?featured.slice(0,4).map(raw=>{const quote=raw.quote??raw,price=numeric(quote.price),id=quote.instrumentId??quote.id,symbol=quote.symbol??id;return `<button class="featured-card historical-quote" data-featured="${esc(id)}" data-symbol="${esc(symbol)}" data-name="${esc(quote.name??symbol)}" data-market="${esc(quote.market??"")}" data-kind="${esc(quote.kind??(quote.market==="CRYPTO"?"crypto":"stock"))}" data-currency="${esc(quote.currency??"")}"><span class="featured-left"><span class="market-badge">${esc(String(symbol??"").slice(0,2))}</span><span><span class="featured-name">${esc(symbol)}<span>${esc(quote.market??"2023 stock")}</span></span><span class="quote-source">${esc(quote.source??"Historical source unavailable")} · ${esc(displayReplayTime(timestamp(quote)))}</span></span></span><span class="featured-right">${price!==null?`<span class="quote-price">${amount(price,2)}<small>${esc(quote.currency??"")}</small></span><div class="quote-change">${esc(historicalPriceBasis(quote))}</div>`:'<span class="quote-unavailable">Historical price unavailable</span>'}</span></button>`;}).join(""):'';
    return;
  }
  const cards=["BTC","ETH"].map(symbol=>{const raw=featured.find(item=>String(item.symbol??item.instrumentId??item.id??item.quote?.instrumentId??"").toUpperCase().includes(symbol));const quote=raw?.quote??raw;const id=quote?.instrumentId??raw?.id??`CRYPTO:${symbol}`;const price=numeric(quote?.price);const name=symbol==="BTC"?"Bitcoin":"Ethereum";const stale=quoteStale(quote);return `<button class="featured-card" data-featured="${esc(id)}" data-symbol="${symbol}" data-name="${name}" title="View ${name} market details"><span class="featured-left"><span class="coin-icon ${symbol==="ETH"?"eth":""}" aria-hidden="true">${symbol==="BTC"?"₿":"◆"}</span><span><span class="featured-name">${name}<span>${symbol} / USD</span></span><span class="quote-source"><span class="status-dot ${price===null?"pending":stale?"pending":"green"}"></span>${esc(price===null?(quote?.error?"Quote unavailable":"Waiting for quote"):quote?.source??"Market data")}${price!==null?` · ${stale?"Delayed":displayTime(timestamp(quote))}`:""}</span></span></span><span class="featured-right">${price===null?'<span class="quote-unavailable">Data unavailable</span>':`<span class="quote-price">${amount(price,2)}<small>${esc(quote.currency??"USD")}</small></span><div class="quote-change ${signClass(quote.changePct)}">${percent(quote.changePct)}${numeric(quote.changePct)!==null?" · 24h":" change unavailable"}</div>`}</span></button>`;});
  $("#featured-quotes").innerHTML=cards.join("");
}
function quoteStale(quote){if(!quote||currentWorld==="historical")return false;const delay=numeric(quote.delaySeconds);return delay!==null?delay>60:Boolean(quote.stale??quote.isStale);}
function renderAgents(){
  const list=[...agents()].sort((a,b)=>(numeric(b.equity)??-Infinity)-(numeric(a.equity)??-Infinity)||String(a.id).localeCompare(String(b.id)));
  $("#agent-table-body").innerHTML=list.length?list.map((agent,index)=>`<tr data-agent="${esc(agent.id)}" tabindex="0" role="button" aria-label="View ${esc(agent.name??agent.id)} portfolio"><td>${String(index+1).padStart(2,"0")}</td><td><div class="agent-cell">${avatar(agent)}<span><span class="agent-name">${esc(agent.name??agent.id)}</span><span class="agent-strategy">${esc(agent.strategy??"Independent investor")}</span></span></div></td><td><span class="model-label ${isSol(agent)?"sol":""}">${esc(agent.model??(isSol(agent)?"gpt-6.1-sol":"gpt-6-astra"))}</span><span class="effort">MAX</span>${riskBadge(agent)}</td><td class="numeric agent-equity">${amount(agent.equity)}</td><td class="numeric ${signClass(agent.pnlPct)}">${percent(agent.pnlPct)}</td><td class="numeric">${amount(agent.cash)}</td><td>${agentStatus(agent)}${updateMetadataHTML(agent)}</td><td class="row-arrow">↗</td></tr>`).join(""):'<tr><td colspan="8" class="table-loading">No investors were returned by the server.</td></tr>';
  $("#chat-investors").innerHTML=agents().map(agent=>`<a href="#conversations" class="chat-investor" data-agent="${esc(agent.id)}">${avatar(agent)}<span><span class="agent-name">${esc(agent.name??agent.id)}</span><span class="agent-strategy">${esc(isSol(agent)?"GPT-6.1 Sol · max":"GPT-6 Astra · max")}</span></span>${agentStatus(agent)}</a>`).join("");
}
function historyPoints(history,agentId=null){
  let list;if(Array.isArray(history))list=history;else if(history&&typeof history==="object")list=Object.entries(history).flatMap(([id,points])=>asArray(points).map(point=>({...point,agentId:point.agentId??id})));else return [];
  list=list.map(point=>({...point,time:parseDate(timestamp(point))?.getTime(),value:firstNumber(point.equity,point.totalEquity,point.value,point.price)})).filter(point=>point.time&&point.value!==null).sort((a,b)=>a.time-b.time);
  if(agentId)return list.filter(point=>(point.agentId??point.agent_id??agentId)===agentId).map(point=>({timestamp:new Date(point.time).toISOString(),value:point.value}));
  if(list.some(point=>!point.agentId&&!point.agent_id))return list.filter(point=>!point.agentId&&!point.agent_id).map(point=>({timestamp:new Date(point.time).toISOString(),value:point.value}));
  const expected=agents().map(agent=>agent.id);const buckets=new Map();list.forEach(point=>{const bucket=Math.floor(point.time/60000)*60000;if(!buckets.has(bucket))buckets.set(bucket,[]);buckets.get(bucket).push(point);});
  const balances=new Map(),points=[];for(const [time,items]of buckets){items.forEach(point=>balances.set(point.agentId??point.agent_id,point.value));if(expected.length&&expected.every(id=>balances.has(id)))points.push({timestamp:new Date(time).toISOString(),value:expected.reduce((total,id)=>total+balances.get(id),0)});}return points;
}
function renderEquity(){
  let points=historyPoints(state?.equityHistory??state?.equity_history);const initial=worldBaseline();
  if(chartMode==="pnl")points=initial===null?[]:points.map(point=>({...point,value:point.value-initial}));
  renderChart($("#equity-chart"),points,{id:"equity",color:"#bce5cd",baseline:chartMode==="equity"?initial:0,emptyTitle:"Waiting for the first snapshot",emptyDescription:"Portfolio history will appear as investors evaluate the markets.",unit:"VC"});
  const warnings=agents().reduce((count,agent)=>count+asArray(agent.valuationWarnings).length,0);
  $("#snapshot-note").textContent=warnings?`${warnings} valuation warning${warnings===1?"":"s"} · see investor details`:`${points.length} recorded snapshot${points.length===1?"":"s"} · available quotes`;
}
function renderChart(container,source,options={}){
  const points=source.map(point=>({time:parseDate(timestamp(point))?.getTime(),value:numeric(point.value??point.price)})).filter(point=>point.time&&point.value!==null).sort((a,b)=>a.time-b.time);
  if(!points.length){container.innerHTML=emptyState(options.emptyTitle??"Price history unavailable",options.emptyDescription??"The data provider has not returned any recorded price points.");return;}
  const width=700,height=215,left=56,right=19,top=18,bottom=28,innerWidth=width-left-right,innerHeight=height-top-bottom;
  const values=points.map(point=>point.value);let minimum=Math.min(...values),maximum=Math.max(...values);
  if(numeric(options.baseline)!==null){minimum=Math.min(minimum,options.baseline);maximum=Math.max(maximum,options.baseline);}
  const span=maximum-minimum;const padding=span?span*.16:Math.max(Math.abs(maximum)*.002,1);minimum-=padding;maximum+=padding;
  const minTime=points[0].time,maxTime=points.at(-1).time;
  const x=(time,index)=>maxTime===minTime?left+innerWidth/2:left+(time-minTime)/(maxTime-minTime)*innerWidth;
  const y=value=>top+(maximum-value)/(maximum-minimum)*innerHeight;
  const coordinates=points.map((point,index)=>[x(point.time,index),y(point.value)]);
  const path=coordinates.map(([pointX,pointY],index)=>`${index?"L":"M"}${pointX.toFixed(2)},${pointY.toFixed(2)}`).join(" ");
  const area=points.length>1?`${path} L${coordinates.at(-1)[0]},${height-bottom} L${coordinates[0][0]},${height-bottom} Z`:"";
  const lines=Array.from({length:4},(_,index)=>{const value=maximum-(maximum-minimum)*index/3;const pointY=y(value);return `<line x1="${left}" y1="${pointY}" x2="${width-right}" y2="${pointY}" stroke="#2b3c2f" stroke-dasharray="3 5"/><text x="${left-10}" y="${pointY+3}" text-anchor="end" fill="#6b8773" font-size="9" font-family="system-ui">${esc(shortAmount(value))}</text>`;}).join("");
  const labels=points.length>1?[0,Math.floor((points.length-1)/2),points.length-1]:[0];
  const dateLabels=[...new Set(labels)].map((index)=>{const point=points[index];const multipleDays=maxTime-minTime>86400000;const label=new Date(point.time).toLocaleString("en-GB",{timeZone:"Asia/Shanghai",...(multipleDays?{day:"2-digit",month:"short"}:{hour:"2-digit",minute:"2-digit"})});return `<text x="${x(point.time,index)}" y="${height-7}" text-anchor="middle" fill="#6b8773" font-size="9" font-family="system-ui">${esc(label)}</text>`;}).join("");
  const baseline=numeric(options.baseline)!==null?`<line x1="${left}" y1="${y(options.baseline)}" x2="${width-right}" y2="${y(options.baseline)}" stroke="#567260" stroke-dasharray="4 5" opacity=".6"/>`:"";
  const id=String(options.id??"chart").replace(/[^a-zA-Z0-9_-]/g,"");
  container.innerHTML=`<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="${esc(options.unit??"")} chart with ${points.length} recorded point${points.length===1?"":"s"}; latest value ${esc(amount(points.at(-1).value,2))}"><defs><linearGradient id="${id}-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${options.color??"#bce5cd"}" stop-opacity=".17"/><stop offset="100%" stop-color="${options.color??"#bce5cd"}" stop-opacity="0"/></linearGradient></defs>${lines}${baseline}${area?`<path d="${area}" fill="url(#${id}-fill)"/>`:""}${points.length>1?`<path d="${path}" fill="none" stroke="${options.color??"#bce5cd"}" stroke-width="2" vector-effect="non-scaling-stroke"/>`:""}<circle cx="${coordinates.at(-1)[0]}" cy="${coordinates.at(-1)[1]}" r="3" fill="${options.color??"#bce5cd"}"/>${dateLabels}<line class="chart-guide" x1="0" y1="${top}" x2="0" y2="${height-bottom}" stroke="#779984" stroke-dasharray="3 4" opacity="0"/><rect x="${left}" y="${top}" width="${innerWidth}" height="${innerHeight}" fill="transparent"/></svg><div class="chart-tooltip"></div>`;
  const svg=$("svg",container),tooltip=$(".chart-tooltip",container),guide=$(".chart-guide",container);
  const move=event=>{const rect=svg.getBoundingClientRect(),position=(event.clientX-rect.left)/rect.width*width;const nearest=coordinates.reduce((best,coordinate,index)=>Math.abs(coordinate[0]-position)<Math.abs(coordinates[best][0]-position)?index:best,0);const point=points[nearest],coordinate=coordinates[nearest];guide.setAttribute("x1",coordinate[0]);guide.setAttribute("x2",coordinate[0]);guide.setAttribute("opacity",".65");tooltip.innerHTML=`<small>${esc(displayDateTime(new Date(point.time).toISOString()))} · Shanghai</small>${amount(point.value,2)} ${esc(options.unit??"")}`;tooltip.style.display="block";tooltip.style.left=`${Math.max(8,Math.min(event.clientX-container.getBoundingClientRect().left+10,container.clientWidth-190))}px`;tooltip.style.top="20px";};
  svg.addEventListener("pointermove",move);svg.addEventListener("pointerleave",()=>{tooltip.style.display="none";guide.setAttribute("opacity","0");});
}

function coverageText(coverage){
  if(!coverage)return "Exchange catalog coverage unavailable";
  if(typeof coverage==="string")return coverage;
  if(Array.isArray(coverage))return coverage.map(item=>`${item.name??item.market??item.id??"Market"}: ${item.status??item.count??"unavailable"}`).join(" · ");
  if(coverage.summary)return coverage.summary;
  if(coverage.message)return coverage.message;
  const data=coverage.markets??coverage;return Object.entries(data).filter(([key,value])=>["string","number"].includes(typeof value)||value&&typeof value==="object").map(([key,value])=>typeof value==="object"?`${value.name??key}: ${numeric(value.count??value.loaded??value.total)!==null?amount(value.count??value.loaded??value.total):"—"} (${String(value.status??(value.complete?"complete":"partial")).replaceAll("_"," ")})`:`${key}: ${value}`).join(" · ")||"Coverage reported by the market provider";
}
function coverageSourceText(coverage){if(!coverage||typeof coverage!=="object")return coverageText(coverage);return Object.entries(coverage.markets??coverage).map(([market,data])=>data&&typeof data==="object"?`${data.name??market}: ${data.count??"?"} symbols; ${data.status??(data.complete?"complete":"partial")}; source: ${data.source??"unavailable"}; source date: ${displayDateTime(data.asOf)} Shanghai${data.error?`; ${data.error}`:""}`:`${market}: ${data}`).join("\n");}
async function loadCatalog(){
  const request=++catalogRequest;$("#instruments-body").innerHTML='<tr><td colspan="4" class="table-loading">Loading instruments…</td></tr>';$("#catalog-prev").disabled=true;$("#catalog-next").disabled=true;
  try{const query=new URLSearchParams({q:$("#instrument-search").value.trim(),market:$("#market-filter").value,limit:String(pageSize),offset:String(catalogOffset)});const data=await api(`/api/instruments?${query}`);if(request!==catalogRequest)return;catalogItems=asArray(data?.items);catalogTotal=firstNumber(data?.total,0);catalogLoaded=true;$("#catalog-total").textContent=`${amount(catalogTotal)} instruments`;$("#catalog-coverage").textContent=coverageText(data?.coverage??state?.coverage);$("#catalog-coverage").title=coverageSourceText(data?.coverage??state?.coverage);renderCatalog();}
  catch(error){if(request!==catalogRequest)return;$("#instruments-body").innerHTML=`<tr><td colspan="4" class="table-loading">${esc(error.message)}<br><button class="button small secondary" data-retry-catalog="true" style="margin-top:12px">Try again</button></td></tr>`;$("#pagination-label").textContent="Catalog unavailable";}
}
function renderCatalog(){
  $("#instruments-body").innerHTML=catalogItems.length?catalogItems.map(item=>`<tr data-instrument="${esc(item.id??item.instrumentId)}" tabindex="0" role="button" aria-label="View ${esc(item.symbol??item.name)} quote" class="${selectedInstrument?.id===(item.id??item.instrumentId)?"selected":""}"><td><span class="instrument-symbol">${esc(item.symbol??item.id)}</span><span class="instrument-company">${esc(item.name??"Name unavailable")}</span></td><td><span class="market-label">${esc(item.market??"—")}</span></td><td>${esc(item.currency??"—")}</td><td class="row-arrow">↗</td></tr>`).join(""):'<tr><td colspan="4" class="table-loading">No matching instruments. Try another symbol or market.</td></tr>';
  $("#pagination-label").textContent=catalogTotal?`${amount(catalogOffset+1)}–${amount(Math.min(catalogOffset+pageSize,catalogTotal))} of ${amount(catalogTotal)}`:"0 instruments";
  $("#pagination-page").textContent=`${Math.floor(catalogOffset/pageSize)+1} / ${Math.max(1,Math.ceil(catalogTotal/pageSize))}`;$("#catalog-prev").disabled=catalogOffset===0;$("#catalog-next").disabled=catalogOffset+pageSize>=catalogTotal;
}
function cachedInstrument(item){
  const id=item.id??item.instrumentId;
  if(!instrumentCache.has(id))instrumentCache.set(id,{id,item:{...item,id},quote:null,quoteError:null,history:[],historyError:null,historyLoaded:false,ready:false,quotePromise:null,historyPromise:null});
  const cache=instrumentCache.get(id);cache.item={...cache.item,...item,id};return cache;
}
function stateQuote(id){const raw=asArray(state?.featured).find(item=>(item.instrumentId??item.id??item.quote?.instrumentId)===id);const quote=raw?.quote??raw;return numeric(quote?.price)!==null?quote:null;}
function loadInstrumentQuote(cache,useSnapshot=false){
  if(cache.quotePromise)return cache.quotePromise;
  const snapshot=useSnapshot?stateQuote(cache.id):null;
  if(snapshot){if(!cache.quote||dateMillis(snapshot)>=dateMillis(cache.quote))cache.quote=snapshot;cache.quoteError=null;return Promise.resolve(cache.quote);}
  cache.quotePromise=api(`/api/quote/${encodeURIComponent(cache.id)}`).then(quote=>{cache.quote=quote;cache.quoteError=null;return quote;}).catch(error=>{cache.quoteError=error.message;return null;}).finally(()=>{cache.quotePromise=null;});
  return cache.quotePromise;
}
function loadInstrumentHistory(cache){
  if(cache.historyLoaded)return Promise.resolve(cache.history);
  if(cache.historyPromise)return cache.historyPromise;
  cache.historyPromise=api(`/api/history/${encodeURIComponent(cache.id)}`).then(history=>{cache.history=Array.isArray(history)?history:asArray(history?.items??history?.history);cache.historyError=null;return cache.history;}).catch(error=>{cache.historyError=error.message;return [];}).finally(()=>{cache.historyLoaded=true;cache.historyPromise=null;});
  return cache.historyPromise;
}
async function selectInstrument(item){
  selectedInstrument={...item,id:item.id??item.instrumentId};const request=++quoteRequest,cache=cachedInstrument(selectedInstrument);if(catalogLoaded)renderCatalog();
  const panel=$("#instrument-detail");
  if(cache.ready)renderInstrumentDetail(cache);
  else{delete panel.dataset.instrumentId;panel.innerHTML=`<div class="detail-title"><div><h2>${esc(item.symbol??item.id)}</h2><p>${esc(item.name??"Market instrument")}</p></div><span class="market-label">${esc(item.market??"—")}</span></div><div class="loading-line">Fetching the latest quote and available history…</div>`;}
  await Promise.all([loadInstrumentQuote(cache),loadInstrumentHistory(cache)]);cache.ready=true;
  if(request===quoteRequest&&selectedInstrument?.id===cache.id)renderInstrumentDetail(cache);
}
async function refreshSelectedInstrument(){
  const item=selectedInstrument;if(route!=="markets"||!item)return;
  const cache=cachedInstrument(item);if(!cache.ready)return;
  // /api/state already carries featured quotes; other symbols get one deduplicated request.
  renderInstrumentDetail(cache);
  await loadInstrumentQuote(cache,true);
  if(route==="markets"&&selectedInstrument?.id===cache.id)renderInstrumentDetail(cache);
}
function renderInstrumentDetail(cache){
  if(selectedInstrument?.id!==cache.id)return;
  const panel=$("#instrument-detail"),quote=cache.quote,item={...cache.item,...quote},price=numeric(quote?.price),currency=quote?.currency??item.currency??"USD";
  const list=Array.isArray(state?.markets)?state.markets:asArray(state?.markets?.markets);
  const market=list.find(market=>String(market.id).toUpperCase()===String(item.market??"").toUpperCase());
  const reportedDelay=numeric(quote?.delaySeconds),serverTime=parseDate(currentWorld==="historical"?(state?.worlds??worldsMetadata)?.historical?.replayTime:state?.serverTime)?.getTime(),quoteTime=parseDate(timestamp(quote))?.getTime();
  const delay=serverTime&&quoteTime?Math.max(0,Math.floor((serverTime-quoteTime)/1000)):reportedDelay;
  const stale=quoteStale({...quote,delaySeconds:delay}),fx=state?.fx,historical=currentWorld==="historical";
  const sameInstrument=panel.dataset.instrumentId===cache.id;
  if(!sameInstrument){panel.dataset.instrumentId=cache.id;panel.innerHTML='<div class="detail-title"></div><div class="detail-price"></div><div class="instrument-chart" id="instrument-chart"></div><dl class="detail-data"></dl><div class="instrument-notices"></div>';}
  $(".detail-title",panel).innerHTML=`<div><h2>${esc(item.symbol??item.id)}</h2><p>${esc(item.name??"Market instrument")}</p></div><span class="market-label">${esc(item.market??"—")}</span>`;
  $(".detail-price",panel).innerHTML=price!==null?`<strong>${amount(price,2)}<small>${esc(currency)}</small></strong><span class="quote-change ${signClass(quote.changePct)}">${percent(quote.changePct)}</span>`:'<strong style="font-size:21px" class="muted">Quote unavailable</strong>';
  $(".detail-data",panel).innerHTML=`<div><dt>Quote source</dt><dd>${esc(quote?.source??"Unavailable")}</dd></div><div><dt>${historical?"Price point · 2023 market time":"Quote time · Shanghai"}</dt><dd>${esc(historical?displayReplayTime(timestamp(quote)):displayDateTime(timestamp(quote)))}</dd></div>${historical?`<div><dt>Price basis</dt><dd>${price===null?"Unavailable":esc(historicalPriceBasis(quote))}</dd></div>`:""}<div><dt>${historical?"Age within replay clock":"Quote age"}</dt><dd>${delay===null?"Not reported":`${amount(delay)} seconds`}</dd></div><div><dt>Trading session</dt><dd>${market?market.isOpen?"Open":"Closed":item.kind==="crypto"||String(item.market).toUpperCase()==="CRYPTO"?"Open · 24/7":"See exchange session"}</dd></div>${currency==="USD"?`<div><dt>USD → CNY conversion</dt><dd>${numeric(fx?.rate)!==null?amount(fx.rate,4):"Unavailable"}</dd></div><div><dt>FX source</dt><dd>${esc(fx?.source??"Unavailable")}</dd></div><div><dt>${historical?"FX source market date":"FX time · Shanghai"}</dt><dd>${esc(historical?displayReplayTime(timestamp(fx)):displayDateTime(timestamp(fx)))}</dd></div>`:""}`;
  $(".instrument-notices",panel).innerHTML=`${stale?'<div class="data-warning">This quote is delayed. Execution uses server checks for quote and exchange-rate freshness.</div>':""}${cache.quoteError?`<div class="detail-error">${quote?"Could not refresh the quote. The last available quote is shown. ":""}${esc(cache.quoteError)}</div>`:""}`;
  const chart=$("#instrument-chart",panel),signature=JSON.stringify([cache.history,cache.historyError,currency]);
  if(!sameInstrument||chart.dataset.historySignature!==signature){renderChart(chart,cache.history.map(point=>({...point,value:point.price??point.value})),{id:"instrument",unit:currency,color:"#bce5cd",emptyDescription:cache.historyError??"No price history has been recorded for this instrument yet."});chart.dataset.historySignature=signature;}
}
function chatState(){return state?.chat??{groups:state?.groups??[],rooms:state?.rooms??[],activeRoom:state?.activeRoom??null,activeRooms:state?.activeRooms??[]};}
function chatGroup(id){return asArray(chatState().groups).find(group=>group.id===id);}
function recipientName(id){return chatGroup(id)?.name??agentName(id);}
function messageInvolves(message,id){return (message.sender??message.senderId??message.agentId)===id||message.recipient===id||message.recipient==="all"||asArray(chatGroup(message.recipient)?.members).includes(id);}
function messageScope(message){if(message.scope)return message.scope;if(message.kind==="public_speech")return "speech";return message.recipient==="all"?"public":String(message.recipient??"").startsWith("group:")?"group":"direct";}
function roomName(id){return asArray(chatState().rooms).find(room=>room.id===id)?.name??id??"";}
function visibleMessage(message){return !message?.archived&&message?.kind!=="invitation"&&!(message?.sender==="arena"&&String(message?.content??"").trim()==="Anyone want to speak?");}
function maybeOpenChatRoom(){pendingChatRoomId=null;}
function renderChatRoom(){
  const chat=chatState(),room=chat.activeRoom,activeRooms=asArray(chat.activeRooms);
  $("#chat-room-name").textContent=room?.name??"Public conversation floor";
  $("#chat-room-invitation").textContent=`${activeRooms.length?`${activeRooms.length} investor rooms active · `:""}You can review all messages; direct and group messages reach only their intended investors.`;
}
function updateComposerVisibility(){
  const recipient=$("#message-recipient").value,group=chatGroup(recipient);const speech=messageKind==="public_speech";
  if(speech){$("#message-recipient").value="all";$("#message-recipient").disabled=true;}else $("#message-recipient").disabled=false;
  $("#composer-visibility").textContent=speech?"Public speech: every investor can receive it.":group?`Group: ${asArray(group.members).map(agentName).join(", ")}. Dashboard owner can review.`:recipient==="all"?"Public: every investor can receive this message.":`Direct: only ${agentName(recipient)} receives this message. Dashboard owner can review.`;
}
function renderRecipients(){
  const groups=asArray(chatState().groups),signature=[...agents().map(agent=>`${agent.id}:${agent.name}`),...groups.map(group=>`${group.id}:${group.name}:${asArray(group.members).join(",")}`)].join("|");
  for(const [selector,initial]of [["#message-recipient",'<option value="all">Everyone · public</option>'],["#chat-filter",'<option value="">All investors / groups</option>']]){const select=$(selector),value=select.value;if(select.dataset.signature===signature)continue;select.innerHTML=initial+agents().map(agent=>`<option value="${esc(agent.id)}">${esc(agent.name??agent.id)} · direct</option>`).join("")+groups.map(group=>`<option value="${esc(group.id)}">${esc(group.name)} · group (${asArray(group.members).length})</option>`).join("");select.value=[...select.options].some(option=>option.value===value)?value:selector==="#message-recipient"?"all":"";select.dataset.signature=signature;}
  updateComposerVisibility();
}
function messageHTML(message){
  if(!visibleMessage(message))return "";
  const sender=message.sender??message.senderId??message.agentId,recipient=message.recipient??"all",kind=message.kind??"message",scope=messageScope(message),agent=agentById(sender),group=chatGroup(recipient),invitation=kind==="invitation";
  const visibility=scope==="direct"?"Direct · sender + recipient":scope==="group"?`Group · ${asArray(group?.members).map(agentName).join(", ")||"selected members"}`:scope==="speech"?"Public speech · all investors":"Public · all investors";
  const kindLabel={tip:"Research tip",distraction:"Distraction",message:"Message",public_speech:"Public speech",invitation:"Arena invitation"}[kind]??kind;
  return `<article class="message ${invitation?"arena-invitation":""}">${sender==="arena"?'<span class="avatar arena">◈</span>':avatar(agent,sender==="human")}<div class="message-body"><div class="message-header"><strong>${esc(sender==="arena"?"Arena":agentName(sender))}</strong><span class="message-recipient">to ${esc(recipientName(recipient))}</span><span class="message-kind ${kind==="distraction"?"distraction":""}">${esc(kindLabel)}</span><time datetime="${esc(timestamp(message))}" title="${esc(displayDateTime(timestamp(message)))} · Shanghai">${displayTime(timestamp(message))}</time></div><div class="message-visibility"><span>${esc(visibility)}</span>${message.roomId??message.room_id?`<span>${esc(roomName(message.roomId??message.room_id))}</span>`:""}</div><p class="message-text">${publicTextHTML(message.content??message.text??"")}</p></div></article>`;
}
function renderMessages(){
  const feed=$("#conversation-feed"),filter=$("#chat-filter").value,all=asArray(state?.messages).filter(visibleMessage);$("#message-count").textContent=all.length>99?"99+":all.length;
  const list=all.filter(message=>{const scope=messageScope(message),group=chatGroup(message.recipient);const involved=!filter||(String(filter).startsWith("group:")?message.recipient===filter:messageInvolves(message,filter));return involved&&(chatScope==="all"||scope===chatScope);}).sort((a,b)=>dateMillis(a)-dateMillis(b));
  const atBottom=feed.scrollHeight-feed.scrollTop-feed.clientHeight<60;const html=list.length?list.map(messageHTML).join(""):emptyState("The floor is open",filter||chatScope!=="all"?"No messages match this investor and visibility filter.":"Investor messages and public speeches will appear here as personal cycles continue.","↗");
  if(feed.dataset.rendered!==html){const top=feed.scrollTop;feed.innerHTML=html;feed.dataset.rendered=html;feed.scrollTop=atBottom?feed.scrollHeight:top;}
}
function eventCategory(event){const kind=String(event.kind??event.type??"").toLowerCase();if(kind==="response_completed")return "response";if(kind==="decision")return "decision";if(kind==="knowledge_updated")return "knowledge";if(/error|fail|reject/.test(kind))return "error";if(/trade|order|buy|sell/.test(kind))return "trade";if(/search|research|web|quote|tool/.test(kind))return "research";if(/message|distraction|tip/.test(kind))return "message";return "event";}
function tradeHTML(trade,mini=false){const action=String(trade.action??trade.side??"Trade").toUpperCase();const instrument=trade.symbol??trade.instrumentId??trade.instrument_id??"Instrument";const agent=trade.agentId??trade.agent_id;return `<div class="${mini?"mini-trade":"activity-item"}">${mini?"":'<span class="activity-icon">⇄</span>'}<div class="activity-body"><div class="activity-header"><strong>${esc(agentName(agent))} <span class="${action==="BUY"?"positive":action==="SELL"?"negative":""}">${esc(action)}</span> ${esc(instrument)}</strong><span class="activity-tag">Trade</span><time title="${esc(displayDateTime(timestamp(trade)))} · Shanghai">${displayTime(timestamp(trade))}</time></div><div class="trade-detail"><span>Quantity <strong>${quantity(trade.quantity)}</strong></span><span>Value <strong>${amount(firstNumber(trade.amountCny,trade.totalCny,trade.notionalCny,trade.costCny,trade.valueCny),2)} VC</strong></span>${firstNumber(trade.priceCny,numeric(trade.price)!==null&&numeric(trade.fxRate)!==null?Number(trade.price)*Number(trade.fxRate):null)!==null?`<span>Price <strong>${amount(firstNumber(trade.priceCny,Number(trade.price)*Number(trade.fxRate)),2)} VC</strong></span>`:""}</div>${trade.reason?`<p class="activity-content">${esc(trade.reason)}</p>`:""}</div></div>`;}
function safeWebLink(value){try{const url=new URL(value);return ["http:","https:"].includes(url.protocol)?url.href:null;}catch{return null;}}
function renderActivity(){
  const filter=$("#activity-filter").value;const trades=asArray(state?.trades);const tradeKeys=new Set(trades.map(trade=>String(trade.id??"")));
  const entries=[...trades.map(trade=>({...trade,_category:"trade",_trade:true})),...asArray(state?.events).filter(event=>!(eventCategory(event)==="trade"&&tradeKeys.has(String(event.details?.tradeId??event.details?.id??"")))).map(event=>({...event,_category:eventCategory(event)}))].filter(event=>filter==="all"||event._category===filter).sort((a,b)=>dateMillis(b)-dateMillis(a)).slice(0,100);
  const html=entries.length?entries.map(event=>{if(event._trade)return tradeHTML(event);if(["response_completed","decision"].includes(event.kind))return responseCardHTML(event);const category=event._category,agent=event.agentId??event.agent_id??event.sender;const details=typeof event.details==="object"&&event.details!==null?event.details:{};const links=asArray(details.results??details.sources).map(source=>{const url=safeWebLink(source.url??source.link??source.href);return url?`<a class="research-link" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(source.title??source.name??url)} ↗</a>`:"";}).join("<br>");const url=safeWebLink(details.url??details.link);return `<article class="activity-item"><span class="activity-icon ${category}">${category==="error"?"!":category==="research"?"⌕":category==="message"?"↗":"◈"}</span><div class="activity-body"><div class="activity-header"><strong>${esc(agent?agentName(agent):"Arena")}</strong><span class="activity-tag">${esc(event.kind??event.type??"Event")}</span><time title="${esc(displayDateTime(timestamp(event)))} · Shanghai">${displayTime(timestamp(event))}</time></div><p class="activity-content">${esc(event.content??event.message??event.description??details.query??"Event recorded")}</p>${links}${url?`<a class="research-link" href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(details.title??"View research source")} ↗</a>`:""}</div></article>`;}).join(""):emptyState(filter==="all"?"The next decision starts here":"No matching activity yet",filter==="all"?"Research, trading decisions, and execution checks will appear when you start the arena.":"New activity will appear here as investors evaluate the markets.","⇄");
  const feed=$("#activity-feed");if(feed.dataset.rendered!==html){feed.innerHTML=html;feed.dataset.rendered=html;}
}
function mergeDetailRecords(previous,incoming,identity){
  const records=new Map();for(const item of [...asArray(previous),...asArray(incoming)])records.set(identity(item),item);
  return [...records.values()].sort((a,b)=>dateMillis(a)-dateMillis(b));
}
function mergeAgentDetailCache(id,data){
  const previous=agentDetailCache.get(id)??{};
  const merged={...previous,...data,
    trades:mergeDetailRecords(previous.trades,data.trades,item=>item.id!==null&&item.id!==undefined?`id:${item.id}`:`${timestamp(item)}:${item.instrumentId??item.instrument_id}:${item.action}:${item.quantity}`),
    messages:mergeDetailRecords(asArray(previous.messages).filter(visibleMessage),asArray(data.messages).filter(visibleMessage),item=>item.id!==null&&item.id!==undefined?`id:${item.id}`:`${timestamp(item)}:${item.sender??item.senderId}:${item.recipient}:${item.kind}:${item.content}`),
    equityHistory:mergeDetailRecords(previous.equityHistory,data.equityHistory??data.equity_history,item=>`${item.agentId??item.agent_id??id}:${timestamp(item)}`),
    events:mergeDetailRecords(previous.events,data.events,item=>item.id??`${item.kind}:${timestamp(item)}:${item.content}`)
  };
  agentDetailCache.set(id,merged);return merged;
}
function agentDetailFromState(id){
  const agent=agentById(id),cached=agentDetailCache.get(id);if(!agent||!Array.isArray(agent.positions))return cached??null;
  const history=asArray(state.equityHistory??state.equity_history).filter(point=>(point.agentId??point.agent_id??id)===id);
  return mergeAgentDetailCache(id,{agent,positions:agent.positions,trades:asArray(state.trades).filter(trade=>(trade.agentId??trade.agent_id)===id),messages:asArray(state.messages).filter(message=>visibleMessage(message)&&messageInvolves(message,id)),equityHistory:history,events:agentEvents(id),runtimeState:state?.runtime?.agentStates?.[id]??{}});
}
function refreshOpenAgent(){
  if(!selectedAgentId||!$("#agent-dialog").open)return;
  const data=agentDetailFromState(selectedAgentId);if(data)renderAgentDetail(data,selectedAgentId);
}
async function openAgent(id){
  const dialog=$("#agent-dialog"),switched=selectedAgentId!==id,opening=!dialog.open||switched;selectedAgentId=id;const request=++agentRequest,epoch=simulationEpoch,agent=agentById(id);$("#agent-dialog-title").textContent=agent?.name??id;if(switched)agentDetailSignature="";if(!dialog.open)dialog.showModal();
  const current=agentDetailFromState(id);if(current){renderAgentDetail(current,id);if(switched)dialog.scrollTop=0;}
  else $("#agent-dialog-content").innerHTML='<div class="loading-line">Loading portfolio…</div>';
  if(!opening)return;
  try{
    // Fetch the investor's own history once on opening; live state contains only the arena's newest records.
    if(!agentDetailRequests.has(id)){const pending=api(`/api/agents/${encodeURIComponent(id)}`).finally(()=>{if(agentDetailRequests.get(id)===pending)agentDetailRequests.delete(id);});agentDetailRequests.set(id,pending);}
    const data=await agentDetailRequests.get(id);if(epoch!==simulationEpoch)return;mergeAgentDetailCache(id,data);ingestEvents(data?.events);
    if(request!==agentRequest||!dialog.open||selectedAgentId!==id)return;
    renderAgentDetail(agentDetailFromState(id)??agentDetailCache.get(id),id);
  }catch(error){if(request!==agentRequest||!dialog.open||selectedAgentId!==id)return;if(current)toast(`Historical records could not refresh: ${error.message}`,"error");else $("#agent-dialog-content").innerHTML=emptyState("Portfolio unavailable",error.message,"!");}
}
function renderAgentDetail(data,id){
  const signature=JSON.stringify([id,data]);if(signature===agentDetailSignature)return;
  const dialog=$("#agent-dialog"),scrollTop=dialog.scrollTop,horizontal=$$(".table-scroll",dialog).map(element=>element.scrollLeft);
  const agent=data?.agent??agentById(id)??{},positions=asArray(data?.positions??agent.positions),trades=asArray(data?.trades),messages=asArray(data?.messages),warnings=asArray(agent.valuationWarnings??data?.valuationWarnings);
  $("#agent-dialog-title").textContent=agent.name??id;
  $("#agent-dialog-content").innerHTML=`<div class="agent-dialog-metrics"><div><span>Equity · VC</span><strong>${amount(agent.equity)}</strong></div><div><span>Return</span><strong class="${signClass(agent.pnlPct)}">${percent(agent.pnlPct)}</strong></div><div><span>Cash · VC</span><strong>${amount(agent.cash)}</strong></div></div><div class="agent-detail-meta"><span class="model-label ${isSol(agent)?"sol":""}">${esc(agent.model??"Model unavailable")}</span><span class="effort">MAX</span>${agentStatus(agent)}<p>${esc(agent.strategy??"This investor chooses its own investments and research.")}</p></div>${warnings.length?`<div class="portfolio-warning">${warnings.map(warning=>esc(typeof warning==="string"?warning:warning.message??JSON.stringify(warning))).join("<br>")}</div>`:""}<div class="agent-dialog-chart" id="agent-chart"></div><div class="detail-section"><h3>Current holdings <span class="title-count">${positions.length}</span></h3>${positions.length?`<div class="table-scroll"><table class="position-table"><thead><tr><th>Instrument</th><th class="numeric">Quantity</th><th class="numeric">Avg. cost · VC</th><th class="numeric">Value · VC</th></tr></thead><tbody>${positions.map(position=>`<tr><td>${esc(position.symbol??position.instrumentId??position.instrument_id)}</td><td class="numeric">${quantity(position.quantity)}</td><td class="numeric">${amount(firstNumber(position.avgCostCny,position.averageCostCny),2)}</td><td class="numeric">${amount(firstNumber(position.marketValue,position.valueCny,position.marketValueCny,position.value),2)}</td></tr>`).join("")}</tbody></table></div>`:emptyState("No open positions","The investor’s capital is currently held in cash.")}</div><div class="detail-section"><h3>Recent trades</h3>${trades.length?[...trades].sort((a,b)=>dateMillis(b)-dateMillis(a)).slice(0,8).map(trade=>tradeHTML({...trade,agentId:trade.agentId??id},true)).join(""):emptyState("No trades recorded","This investor has not executed an order yet.")}</div>`;
  $(".agent-detail-meta",$("#agent-dialog-content")).insertAdjacentHTML("afterend",publicAgentSection(agent));
  if(messages.length){const section=document.createElement("div");section.className="detail-section";section.innerHTML=`<h3>Recent conversations</h3>${[...messages].sort((a,b)=>dateMillis(b)-dateMillis(a)).slice(0,6).map(message=>`<article class="mini-trade"><div class="activity-header"><strong>${esc(agentName(message.sender??message.senderId??message.agentId))} → ${esc(agentName(message.recipient??"all"))}</strong><span class="message-kind ${message.kind==="distraction"?"distraction":""}">${esc(message.kind??"tip")}</span><time>${displayTime(timestamp(message))}</time></div><p class="activity-content">${esc(message.content??message.text??"")}</p></article>`).join("")}`;$("#agent-dialog-content").append(section);}
  renderChart($("#agent-chart"),historyPoints(data?.equityHistory??data?.equity_history,id),{id:"agent",unit:"VC",color:isSol(agent)?"#96badf":"#bda4e8",baseline:firstNumber(agent.initialCash,currentWorld==="main"?800000:null),emptyTitle:"No performance history yet",emptyDescription:"Recorded portfolio valuations will appear here."});
  agentDetailSignature=signature;dialog.scrollTop=scrollTop;$$(".table-scroll",dialog).forEach((element,index)=>{element.scrollLeft=horizontal[index]??0;});
}
async function simulationAction(action){
  if(busyAction||currentWorld!=="main")return;busyAction=true;renderControls();
  try{const result=await api(`/api/simulation/${action}`,{method:"POST",body:"{}",timeout:action==="round"?60000:30000});toast(action==="start"?"Arena started. All ten investors will participate in each round.":action==="pause"?"Arena paused.":"Round requested. Follow the investors in Activity.");await refreshState(true);if(result?.detail)console.info("Arena:",result.detail);}
  catch(error){toast(error.message,"error");}
  finally{busyAction=false;if(state)renderControls();}
}
function updateResetButton(){const runtime=state?.runtime??{};const paused=currentWorld==="main"&&Boolean(state)&&!runtime.running&&!runtime.inProgress;$("#confirm-reset").disabled=!paused||$("#reset-confirm").value!=="RESET"||busyAction;$("#reset-status").textContent=paused?"The arena is paused. This action clears the recorded simulation.":"Pause the investors and wait for active evaluations to stop before resetting.";}

window.addEventListener("hashchange",changeRoute);
document.addEventListener("click",event=>{
  const world=event.target.closest("[data-world]");if(world){switchWorld(world.dataset.world);return;}
  const agent=event.target.closest("[data-agent]");if(agent){event.preventDefault();openAgent(agent.dataset.agent);return;}
  const instrument=event.target.closest("[data-instrument]");if(instrument){const item=catalogItems.find(item=>(item.id??item.instrumentId)===instrument.dataset.instrument);if(item)selectInstrument(item);return;}
  const featured=event.target.closest("[data-featured]");if(featured){location.hash="markets";selectInstrument({id:featured.dataset.featured,symbol:featured.dataset.symbol,name:featured.dataset.name,market:featured.dataset.market??"CRYPTO",kind:featured.dataset.kind??"crypto",currency:featured.dataset.currency??"USD"});return;}
  const close=event.target.closest("[data-close]");if(close){$(`#${close.dataset.close}`).close();return;}
  const chart=event.target.closest("[data-chart]");if(chart){chartMode=chart.dataset.chart;$$("[data-chart]").forEach(button=>{button.classList.toggle("selected",button===chart);button.setAttribute("aria-pressed",String(button===chart));});if(state)renderEquity();return;}
  const chatTab=event.target.closest("[data-chat-scope]");if(chatTab){chatScope=chatTab.dataset.chatScope;$$("[data-chat-scope]").forEach(button=>{button.classList.toggle("selected",button===chatTab);button.setAttribute("aria-pressed",String(button===chatTab));});renderMessages();return;}
  if(event.target.closest("[data-open-responses]")){$("#activity-filter").value="response";renderActivity();}
  const kind=event.target.closest("[data-kind]");if(kind){messageKind=kind.dataset.kind;$$("[data-kind]").forEach(button=>{button.classList.toggle("selected",button===kind);button.setAttribute("aria-pressed",String(button===kind));});updateComposerVisibility();return;}
  const preset=event.target.closest("[data-interval]");if(preset)$("#round-interval").value=preset.dataset.interval;
  if(event.target.closest("[data-retry-catalog]"))loadCatalog();
});
document.addEventListener("keydown",event=>{const row=event.target.closest("tr[data-agent], tr[data-instrument]");if(row&&(event.key==="Enter"||event.key===" ")){event.preventDefault();row.click();}if(event.key==="/"&&!/INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName)&&!$$("dialog").some(dialog=>dialog.open)){event.preventDefault();location.hash="markets";$("#instrument-search").focus();}});
$("#retry-state").addEventListener("click",()=>refreshState());
$("#toggle-simulation").addEventListener("click",()=>simulationAction((state?.runtime?.running||state?.runtime?.inProgress)?"pause":"start"));
$("#run-round").addEventListener("click",()=>simulationAction("round"));
$("#instrument-search").addEventListener("input",()=>{clearTimeout(searchTimer);searchTimer=setTimeout(()=>{catalogOffset=0;loadCatalog();},260);});
$("#market-filter").addEventListener("change",()=>{catalogOffset=0;loadCatalog();});
$("#catalog-prev").addEventListener("click",()=>{catalogOffset=Math.max(0,catalogOffset-pageSize);loadCatalog();});
$("#catalog-next").addEventListener("click",()=>{catalogOffset+=pageSize;loadCatalog();});
$("#message-recipient").addEventListener("change",updateComposerVisibility);
$("#chat-filter").addEventListener("change",renderMessages);$("#activity-filter").addEventListener("change",renderActivity);
$("#message-form").addEventListener("submit",async event=>{event.preventDefault();const content=$("#message-content").value.trim();if(!content)return;const button=$("#send-message");button.disabled=true;try{await api("/api/messages",{method:"POST",body:JSON.stringify({sender:"human",recipient:$("#message-recipient").value,kind:messageKind,content})});$("#message-content").value="";toast("Message delivered to the conversation floor.");await refreshState(true);const feed=$("#conversation-feed");feed.scrollTop=feed.scrollHeight;}catch(error){toast(error.message,"error");}finally{button.disabled=false;}});
for(const button of $$("#open-settings, #top-settings"))button.addEventListener("click",()=>{if(currentWorld!=="main")return;$("#round-interval").value=state?.runtime?.intervalSeconds??300;$("#settings-dialog").showModal();});
$("#settings-form").addEventListener("submit",async event=>{event.preventDefault();if(currentWorld!=="main")return;const intervalSeconds=Number($("#round-interval").value);if(!Number.isInteger(intervalSeconds)||intervalSeconds<60||intervalSeconds>3600){toast("Choose an interval between 60 and 3,600 seconds.","error");return;}const button=$("#save-settings");button.disabled=true;try{await api("/api/settings",{method:"POST",body:JSON.stringify({intervalSeconds})});$("#settings-dialog").close();toast(`Round interval saved: ${amount(intervalSeconds)} seconds.`);await refreshState(true);}catch(error){toast(error.message,"error");}finally{button.disabled=false;}});
$("#open-reset").addEventListener("click",()=>{$("#reset-confirm").value="";updateResetButton();$("#reset-dialog").showModal();});$("#reset-confirm").addEventListener("input",updateResetButton);
$("#confirm-reset").addEventListener("click",async()=>{updateResetButton();if($("#confirm-reset").disabled)return;busyAction=true;updateResetButton();try{await api("/api/simulation/reset",{method:"POST",body:"{}"});simulationEpoch++;agentDetailCache.clear();agentDetailRequests.clear();eventMemory.clear();agentDetailSignature="";roomInitialized=false;seenRoomIds.clear();$("#reset-dialog").close();$("#settings-dialog").close();toast("Arena reset. Every investor now has 800,000 VC.");await refreshState(true);}catch(error){toast(error.message,"error");}finally{busyAction=false;if(state)renderControls();}});
// A dialog backdrop click dismisses the dialog without triggering its contents.
$$("dialog").forEach(dialog=>dialog.addEventListener("click",event=>{if(event.target!==dialog)return;const rect=dialog.getBoundingClientRect();if(event.clientX<rect.left||event.clientX>rect.right||event.clientY<rect.top||event.clientY>rect.bottom)dialog.close();}));
$("#open-group").addEventListener("click",()=>{$("#group-name").value="";$("#group-members").innerHTML=agents().map(agent=>`<label><input type="checkbox" name="group-member" value="${esc(agent.id)}">${avatar(agent)}<span>${esc(agent.name??agent.id)}</span>${riskBadge(agent)}</label>`).join("");$("#group-dialog").showModal();});
$("#group-form").addEventListener("submit",async event=>{event.preventDefault();const name=$("#group-name").value.trim(),members=$$('input[name="group-member"]:checked').map(input=>input.value);if(!name||members.length<2){toast("Give the group a name and choose at least two investors.","error");return;}const button=$("#create-group");button.disabled=true;try{const result=await api("/api/chat/groups",{method:"POST",body:JSON.stringify({name,members})});$("#group-dialog").close();toast(`Group created: ${name}`);await refreshState(true);const id=result?.id??result?.group?.id;if(id){$("#message-recipient").value=id;updateComposerVisibility();}}catch(error){toast(error.message,"error");}finally{button.disabled=false;}});
document.addEventListener("focusout",()=>setTimeout(maybeOpenChatRoom,150));$$("dialog").forEach(dialog=>dialog.addEventListener("close",maybeOpenChatRoom));
document.addEventListener("visibilitychange",()=>{if(!document.hidden){refreshState(true);if(!streamSource&&!streamReconnectTimer)connectStream();}});
renderWorlds();renderControls();changeRoute();updateClock();refreshState();connectStream();setInterval(()=>{if(!document.hidden&&(streamStatus!=="live"||Date.now()-lastFullRefreshAt>30000))refreshState(true);},7000);setInterval(updateClock,30000);setInterval(updateMergeCountdown,1000);

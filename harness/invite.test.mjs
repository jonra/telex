// Test invite + tag + pages.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
const BASE = "http://127.0.0.1:4123";
let fail = 0; const assert = (c, m) => { console.log(`${c ? "✅" : "❌"} ${m}`); if (!c) fail++; };
const txt = (r) => r.content.map((x) => x.text).join("\n");
async function connect(n){ const c = new Client({name:n,version:"1.0.0"}); await c.connect(new StreamableHTTPClientTransport(new URL(`${BASE}/mcp`))); return c; }

const op = await connect("operator"); const dev = await connect("device"); const app = await connect("app");
await op.callTool({name:"register",arguments:{name:"operator"}});
await dev.callTool({name:"register",arguments:{name:"device",model:"haiku",account:"max-personal",cost:"low"}});
await app.callTool({name:"register",arguments:{name:"app",model:"sonnet",account:"team",cost:"medium"}});

// operator invites device + app to #delivery
const inv = txt(await op.callTool({name:"invite",arguments:{channel:"delivery",agents:["device","app"],message:"new channel delivery"}}));
assert(inv.includes("device") && inv.includes("app"), "invite reports both invitees");

// each invitee received an invitation DM
assert(txt(await dev.callTool({name:"inbox",arguments:{}})).includes("invited you to #delivery"), "device got invitation");
assert(txt(await app.callTool({name:"inbox",arguments:{}})).includes("invited you to #delivery"), "app got invitation");

// channel message now reaches both
await op.callTool({name:"send",arguments:{to:"#delivery",text:"go"}});
assert(txt(await dev.callTool({name:"inbox",arguments:{}})).includes('"text": "go"'), "device receives channel message after invite");

// metadata visible in who()
const who = JSON.parse(txt(await op.callTool({name:"who",arguments:{}})));
const d = who.agents.find(a=>a.name==="device");
assert(d?.meta?.account === "max-personal" && d?.meta?.cost === "low", "device metadata (account/cost) present");

// operator control invite endpoint
const ci = await (await fetch(`${BASE}/api/control/invite`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({channel:"ops",agents:["device"]})})).json();
assert(ci.ok && ci.members.includes("device"), "POST /api/control/invite adds member");

// pages
const home = await (await fetch(`${BASE}/`)).text();
assert(/Connect to dashboard/.test(home), "landing page serves with Connect button");
const dash = await (await fetch(`${BASE}/dashboard`)).text();
assert(/TELEX/.test(dash) && /id="net"/.test(dash), "/dashboard serves the dashboard");

await op.close(); await dev.close(); await app.close();
console.log(fail ? `\n${fail} FAILED` : "\nALL PASSED"); process.exit(fail?1:0);

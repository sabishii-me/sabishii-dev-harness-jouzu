import { approvalContext } from "./approval-context.mjs";
// agent-presets — a harness-side extension that gives a harness a preset
// mechanism it lacks natively (pi/jouzu).
//
// It is a plain pi/jouzu extension, not part of the hub: it knows nothing about
// the hub. The manager writes a preset definition file and points
// AGENT_PRESETS_CONFIG at it; this extension applies that preset:
//   - systemPrompt  → appended as a system-prompt section
//   - tools         → allow-list; a tool_call outside it is blocked
//   - approve       → ask before every tool call (the user answers in the hub)
//
// A preset can enable approval initially. The explicit /review control can
// enable or disable it later; loading the extension alone does not enable it.
//
// The definition file is JSON:
//   { "active": "<preset-id>",
//     "presets": { "<id>": { systemPrompt?, tools?[], approve? } } }
//
// Without a definition, no preset policy is applied and review starts off.
// The explicit /review command is still registered when this extension is loaded.
import fs from "node:fs";

const CONFIG_ENV = "AGENT_PRESETS_CONFIG";

function readConfig() {
  const p = process.env[CONFIG_ENV];
  if (!p) return null;
  try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return null; }
}

function activePreset() {
  const cfg = readConfig();
  if (!cfg || !cfg.active) return null;
  return (cfg.presets && cfg.presets[cfg.active]) || null;
}

export default function agentPresets(pi) {
  // Hub installation exposes the explicit review control; merely loading it
  // never enables approval. A selected preset may provide an initial policy.
  const preset = activePreset() || {};

  const allowed = Array.isArray(preset.tools) && preset.tools.length ? new Set(preset.tools) : null;
  const presetId = readConfig()?.active ?? null;

  if (typeof preset.systemPrompt === "string" && preset.systemPrompt.trim()) {
    pi.on("before_agent_start", async () => ({ systemPrompt: preset.systemPrompt }));
  }

  // The preset asks before every tool: one handler that both enforces the
  // allow-list and obtains consent, so a blocked-by-preset tool never even
  // reaches a prompt and an allowed tool is confirmed before it runs.
  //
  // `approve` is the preset's starting point, not a nail: a session that has
  // started cannot change preset, so the asking has to be switchable within the
  // session. `/review off` lets the confirm step through unanswered (the
  // allow-list still applies — that is the preset, not the review), and
  // `/review on` asks again. The switch is per session and lives in the log.
  const review = { on: preset.approve === true };
  const FIELD = "hub-review/state";

  {
    pi.on("tool_call", async (event, ctx) => {
      const name = event && event.toolName;
      if (typeof name === "string" && name && allowed && !allowed.has(name)) {
        return { block: true, reason: `tool '${name}' is not enabled in preset '${presetId}'` };
      }
      const policy = { event, blocks: [] };
      pi.events.emit("tool-policy/preflight", policy);
      if (policy.blocks.length) return { block: true, reason: policy.blocks.join("; ") };
      if (review.on) {
        const context = await approvalContext(event, ctx.cwd);
        // Input's string result preserves a structured decision; confirm's boolean
        // cannot distinguish an expired request from an explicit human denial.
        const raw = await ctx.ui.input("tool-review/v2", JSON.stringify(context));
        let decision;
        try { decision = JSON.parse(raw || "null"); } catch {}
        const source = decision?.source;
        pi.appendEntry("hub-review/decision", { toolCallId: event.toolCallId, source: source || "unavailable", approved: decision?.approved === true, at: new Date().toISOString() });
        if (decision?.approved === true && source === "user") return;
        if (source === "timeout") return { block: true, reason: `Approval timed out for '${name}'; no user decision was received. This is NOT a user rejection. Do not automatically retry this operation.` };
        if (source === "user") return { block: true, reason: `The user explicitly denied this '${name}' operation.` };
        return { block: true, reason: `Approval could not be obtained for '${name}'. No user rejection was recorded; do not automatically retry.` };
      }
    });
  }

  // The runtime switch is explicit and available whenever the hub installed
  // this extension. It does not enable approval until requested.
  {
    pi.registerCommand("review", {
      description: "Ask before every tool call, or stop asking with '/review off'",
      handler: async (rawArgs, ctx) => {
        const arg = String(rawArgs ?? "").trim().toLowerCase();
        if (arg === "" || arg === "status") {
          ctx.ui?.notify?.(`review is ${review.on ? "on" : "off"}`);
          return;
        }
        if (arg !== "on" && arg !== "off") {
          ctx.ui?.notify?.(`unknown argument "${arg}" (use on, off, or status)`);
          return;
        }
        review.on = arg === "on";
        pi.appendEntry(FIELD, { asking: review.on });
        ctx.ui?.notify?.(`review ${review.on ? "on" : "off"}`);
      },
    });
  }

  // A resumed session comes back in the mode it left in: the newest record wins.
  pi.on("session_start", async (_event, ctx) => {
    try {
      const last = ctx.sessionManager
        .getEntries()
        .filter((e) => e && e.type === "custom" && e.customType === FIELD)
        .pop();
      if (last && last.data && typeof last.data.asking === "boolean") review.on = last.data.asking;
    } catch {}
  });
}

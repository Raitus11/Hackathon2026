import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import * as d3 from "d3";

const API = "http://localhost:8000";



const T = {
  // Core palette
  bg0: "#0B0E11",        // deepest black-blue
  bg1: "#111519",        // panels
  bg2: "#181D23",        // cards
  bg3: "#1F262E",        // elevated cards / hover
  bg4: "#283140",        // active states
  border0: "#1E2530",    // subtle
  border1: "#2A3545",    // visible
  border2: "#3A4A5C",    // prominent

  // Text
  t1: "#F0F2F4",         // primary
  t2: "#A0AABB",         // secondary
  t3: "#5E6D80",         // tertiary
  t4: "#3A4A5C",         // disabled

  // Accents
  cyan: "#00D4FF",
  cyanDim: "#00A0CC",
  cyanBg: "rgba(0,212,255,0.08)",
  cyanBorder: "rgba(0,212,255,0.2)",
  green: "#00E08A",
  greenDim: "#00B870",
  greenBg: "rgba(0,224,138,0.08)",
  greenBorder: "rgba(0,224,138,0.2)",
  red: "#FF4466",
  redDim: "#CC3355",
  redBg: "rgba(255,68,102,0.08)",
  redBorder: "rgba(255,68,102,0.2)",
  amber: "#FFB020",
  amberDim: "#CC8C18",
  amberBg: "rgba(255,176,32,0.08)",
  amberBorder: "rgba(255,176,32,0.2)",
  purple: "#A78BFA",
  purpleBg: "rgba(167,139,250,0.08)",

  // Typography
  fontMono: "'JetBrains Mono', 'Fira Code', 'SF Mono', 'Cascadia Code', monospace",
  fontSans: "'DM Sans', 'General Sans', system-ui, -apple-system, sans-serif",
  fontDisplay: "'Space Grotesk', 'Outfit', 'DM Sans', system-ui, sans-serif",

  // Radii
  r1: "6px",
  r2: "10px",
  r3: "14px",

  // Shadows
  glow: (c, a = 0.3) => `0 0 20px rgba(${c},${a}), 0 0 60px rgba(${c},${a * 0.5})`,
  shadow1: "0 1px 3px rgba(0,0,0,0.4), 0 1px 2px rgba(0,0,0,0.3)",
  shadow2: "0 4px 16px rgba(0,0,0,0.5), 0 2px 4px rgba(0,0,0,0.3)",
  shadow3: "0 12px 40px rgba(0,0,0,0.6), 0 4px 12px rgba(0,0,0,0.4)",
};

// Inject fonts & global styles
const STYLE_TAG = `
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;0,700&family=JetBrains+Mono:wght@300;400;500;600&family=Space+Grotesk:wght@400;500;600;700&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { font-size: 14px; }
body {
  background: ${T.bg0};
  color: ${T.t1};
  font-family: ${T.fontSans};
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  overflow-x: hidden;
}
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: ${T.border2}; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: ${T.t3}; }
::selection { background: rgba(0,212,255,0.25); }

@keyframes fadeUp {
  from { opacity: 0; transform: translateY(12px); }
  to { opacity: 1; transform: translateY(0); }
}
@keyframes fadeIn {
  from { opacity: 0; }
  to { opacity: 1; }
}
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}
@keyframes shimmer {
  0% { background-position: -200% 0; }
  100% { background-position: 200% 0; }
}
@keyframes slideIn {
  from { opacity: 0; transform: translateX(-8px); }
  to { opacity: 1; transform: translateX(0); }
}
@keyframes scaleIn {
  from { opacity: 0; transform: scale(0.95); }
  to { opacity: 1; transform: scale(1); }
}
@keyframes countUp {
  from { opacity: 0; transform: translateY(8px); }
  to { opacity: 1; transform: translateY(0); }
}
@keyframes borderGlow {
  0%, 100% { border-color: rgba(0,212,255,0.2); }
  50% { border-color: rgba(0,212,255,0.5); }
}
@keyframes spin {
  to { transform: rotate(360deg); }
}
`;

/* ═══════════════════════════════════════════════════════════════════════════
   UTILITY COMPONENTS
   ═══════════════════════════════════════════════════════════════════════════ */

function Badge({ children, color = T.cyan, bg, style = {} }) {
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 4,
      padding: "2px 8px", borderRadius: 20,
      fontSize: 11, fontWeight: 600, letterSpacing: "0.02em",
      fontFamily: T.fontMono,
      color,
      background: bg || `${color}15`,
      border: `1px solid ${color}30`,
      ...style,
    }}>{children}</span>
  );
}

function Stat({ label, value, color = T.t1, sub, delay = 0 }) {
  return (
    <div style={{
      animation: `countUp 0.5s ease-out ${delay}s both`,
      textAlign: "center", padding: "16px 12px",
    }}>
      <div style={{
        fontSize: 32, fontWeight: 700, fontFamily: T.fontDisplay,
        color, lineHeight: 1,
        textShadow: color !== T.t1 ? `0 0 30px ${color}40` : "none",
      }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, marginTop: 4 }}>{sub}</div>}
      <div style={{ fontSize: 11, color: T.t3, marginTop: 6, fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.08em" }}>{label}</div>
    </div>
  );
}

function Card({ children, style = {}, glow, hover, delay = 0, ...props }) {
  return (
    <div style={{
      background: T.bg2,
      border: `1px solid ${glow ? `${glow}30` : T.border0}`,
      borderRadius: T.r2,
      overflow: "hidden",
      animation: `fadeUp 0.4s ease-out ${delay}s both`,
      boxShadow: glow ? `0 0 20px ${glow}10` : T.shadow1,
      transition: "border-color 0.2s, box-shadow 0.2s",
      ...style,
    }} {...props}>{children}</div>
  );
}

function CardHeader({ children, right, style = {} }) {
  return (
    <div style={{
      padding: "12px 16px",
      borderBottom: `1px solid ${T.border0}`,
      display: "flex", alignItems: "center", justifyContent: "space-between",
      background: `${T.bg3}60`,
      ...style,
    }}>
      <span style={{ fontSize: 12, fontWeight: 600, color: T.t2, textTransform: "uppercase", letterSpacing: "0.06em" }}>{children}</span>
      {right && <span>{right}</span>}
    </div>
  );
}

function ProgressBar({ value, max = 100, color = T.cyan, height = 4 }) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div style={{ height, background: T.bg4, borderRadius: height / 2, overflow: "hidden" }}>
      <div style={{
        width: `${pct}%`, height: "100%",
        background: `linear-gradient(90deg, ${color}, ${color}CC)`,
        borderRadius: height / 2,
        transition: "width 0.8s cubic-bezier(0.16, 1, 0.3, 1)",
        boxShadow: `0 0 8px ${color}40`,
      }} />
    </div>
  );
}

function DownloadButton({ onClick, label = "Download", icon = "↓" }) {
  return (
    <button onClick={onClick} style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: "6px 14px", borderRadius: T.r1,
      background: "transparent", border: `1px solid ${T.border1}`,
      color: T.t2, fontSize: 11, fontWeight: 500, fontFamily: T.fontMono,
      cursor: "pointer", transition: "all 0.15s",
    }}
      onMouseEnter={e => { e.target.style.borderColor = T.cyan; e.target.style.color = T.cyan; }}
      onMouseLeave={e => { e.target.style.borderColor = T.border1; e.target.style.color = T.t2; }}
    >
      <span style={{ fontSize: 13 }}>{icon}</span> {label}
    </button>
  );
}

function SectionTitle({ children, count, delay = 0 }) {
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 10,
      marginBottom: 16, animation: `fadeIn 0.4s ease-out ${delay}s both`,
    }}>
      <h3 style={{ fontSize: 13, fontWeight: 600, color: T.t2, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: T.fontSans }}>{children}</h3>
      {count != null && <Badge color={T.t3}>{count}</Badge>}
      <div style={{ flex: 1, height: 1, background: T.border0 }} />
    </div>
  );
}

function EmptyState({ icon = "◇", message }) {
  return (
    <div style={{
      padding: "60px 20px", textAlign: "center",
      animation: "fadeIn 0.5s ease-out",
    }}>
      <div style={{ fontSize: 36, marginBottom: 12, opacity: 0.3 }}>{icon}</div>
      <div style={{ fontSize: 13, color: T.t3, maxWidth: 320, margin: "0 auto", lineHeight: 1.6 }}>{message}</div>
    </div>
  );
}


function CollapsibleSection({ title, icon, color = T.cyan, count, subtitle, defaultOpen = false, delay = 0, children }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Card style={{ marginBottom: 12 }} glow={open ? color : undefined} delay={delay}>
      <div
        onClick={() => setOpen(!open)}
        style={{
          padding: "14px 16px", cursor: "pointer",
          display: "flex", alignItems: "center", gap: 12,
          background: open ? `${color}08` : "transparent",
          transition: "background 0.2s",
        }}
        onMouseEnter={e => { if (!open) e.currentTarget.style.background = `${T.bg3}40`; }}
        onMouseLeave={e => { if (!open) e.currentTarget.style.background = open ? `${color}08` : "transparent"; }}
      >
        <span style={{ fontSize: 14, color, flexShrink: 0 }}>{icon}</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: T.t1 }}>{title}</span>
            {count != null && <Badge color={color} style={{ fontSize: 9 }}>{count} {count === 1 ? "file" : "files"}</Badge>}
          </div>
          {subtitle && <div style={{ fontSize: 11, color: T.t3, marginTop: 2 }}>{subtitle}</div>}
        </div>
        <span style={{
          fontSize: 10, color: T.t4,
          transform: open ? "rotate(180deg)" : "rotate(0deg)",
          transition: "transform 0.2s",
        }}>▼</span>
      </div>
      {open && (
        <div style={{ borderTop: `1px solid ${T.border0}`, animation: "fadeIn 0.2s" }}>
          {children}
        </div>
      )}
    </Card>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   D3 TOPOLOGY GRAPH
   ═══════════════════════════════════════════════════════════════════════════ */

const NODE_COLORS = {
  qm:    T.cyan,
  app:   T.green,
  queue: T.amber,
};

/* ═══════════════════════════════════════════════════════════════════════════
   TopologyGraph — DROP-IN REPLACEMENT
   ═══════════════════════════════════════════════════════════════════════════
   
   INSTALLATION:
   1. Open your App.jsx
   2. Find the existing TopologyGraph function (search for "function TopologyGraph")
   3. Replace the ENTIRE function (from "function TopologyGraph" to its closing "}")
      with the new function below
   4. That's it. No other changes needed. Same props, same API.
   
   WHAT CHANGED:
   - AS-IS graphs use a dense force layout (tuned for 150+ nodes)
   - TARGET graphs use a radial concentric ring layout showing 1:1 QM-per-app
   - Layout mode auto-detected from the `title` prop (or explicit `isTarget` prop)
   - Queue nodes supported when showQueues=true
   - Zoom-to-fit after simulation settles
   - Legend updates to reflect dedicated vs shared QMs
   
   ═══════════════════════════════════════════════════════════════════════════ */

// ── Paste this function to REPLACE the existing TopologyGraph ─────────────

function TopologyGraph({ graphData, title, height = 360, badge, showQueues = false, isTarget }) {
  const svgRef = useRef(null);
  const containerRef = useRef(null);

  // Auto-detect if this is a target graph from title if isTarget not explicitly set
  const targetMode = isTarget !== undefined ? isTarget
    : /target|proposed|optimis|new/i.test(title || "");

  useEffect(() => {
    if (!graphData?.nodes?.length || !svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const w = containerRef.current?.clientWidth || 600;
    const h = height;

    // ── Filter nodes/edges based on showQueues ─────────────────────────
    const nodes = graphData.nodes
      .filter(d => showQueues ? true : (d.type === "qm" || d.type === "app"))
      .map(d => ({ ...d }));
    const nodeIds = new Set(nodes.map(n => n.id));
    const edges = graphData.edges
      .filter(d => {
        const src = typeof d.source === "object" ? d.source.id : d.source;
        const tgt = typeof d.target === "object" ? d.target.id : d.target;
        if (!nodeIds.has(src) || !nodeIds.has(tgt)) return false;
        if (showQueues) return true;
        return d.rel === "channel" || d.rel === "connects_to";
      })
      .map(d => ({ ...d }));

    const g = svg.append("g");
    const zoom = d3.zoom().scaleExtent([0.1, 5]).on("zoom", e => g.attr("transform", e.transform));
    svg.call(zoom);

    // ── Shared: Defs (arrow markers, glow filters) ─────────────────────
    const defs = svg.append("defs");
    ["qm", "app", "queue"].forEach(type => {
      const filter = defs.append("filter").attr("id", `glow-${type}-${title}`);
      filter.append("feGaussianBlur").attr("stdDeviation", 3).attr("result", "blur");
      filter.append("feMerge").selectAll("feMergeNode")
        .data(["blur", "SourceGraphic"]).join("feMergeNode")
        .attr("in", d => d);
    });
    ["channel", "connects_to", "owns"].forEach(rel => {
      defs.append("marker")
        .attr("id", `arrow-${rel}-${title}`)
        .attr("viewBox", "0 0 10 10")
        .attr("refX", 25).attr("refY", 5)
        .attr("markerWidth", 5).attr("markerHeight", 5)
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M1 2L7 5L1 8")
        .attr("fill", rel === "channel" ? T.cyan : rel === "owns" ? T.amber : T.green)
        .attr("opacity", 0.6);
    });

    // ── Choose layout strategy ─────────────────────────────────────────
    // Filtered subgraph (app trace active) → flow layout (detailed MQ objects)
    // Full (unfiltered) target graph → radial concentric rings
    // AS-IS → dense force directed
    const qmCount = nodes.filter(n => n.type === "qm").length;
    const isFiltered = /—|FILTERED/i.test(title || "");
    const flowThreshold = isFiltered ? 50 : 15;
    if (targetMode && qmCount <= flowThreshold && qmCount > 0) {
      drawTargetFlow(g, nodes, edges, w, h, svg, zoom, title, showQueues);
    } else if (targetMode) {
      drawTargetRadial(g, nodes, edges, w, h, svg, zoom, title, showQueues);
    } else {
      drawAsIsForce(g, nodes, edges, w, h, svg, zoom, title, showQueues);
    }

  }, [graphData, height, title, showQueues, targetMode]);

  // ── Legend entries ────────────────────────────────────────────────────
  const legendItems = targetMode
    ? [
        ["Queue Manager", T.cyan, "◆", "(dedicated 1:1)"],
        ["Application", T.green, "●", ""],
      ]
    : [
        ["Queue Manager", T.cyan, "◆", "(shared)"],
        ["Application", T.green, "●", ""],
      ];
  if (showQueues) {
    legendItems.push(["Local Q", T.amber, "▪", ""]);
    legendItems.push(["Remote Q", T.amber, "↗", ""]);
    legendItems.push(["XMITQ", T.purple, "⇄", ""]);
  }

  return (
    <div ref={containerRef} style={{ background: T.bg1, borderRadius: T.r2, border: `1px solid ${T.border0}`, overflow: "hidden" }}>
      <div style={{
        padding: "10px 14px", display: "flex", alignItems: "center", justifyContent: "space-between",
        borderBottom: `1px solid ${T.border0}`, background: `${T.bg3}40`,
      }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: T.t3, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: T.fontMono }}>{title}</span>
        {badge}
      </div>
      <svg ref={svgRef} width="100%" height={height} style={{ display: "block" }} />
      <div style={{ padding: "8px 14px", display: "flex", gap: 16, borderTop: `1px solid ${T.border0}` }}>
        {legendItems.map(([label, color, icon, sub]) => (
          <span key={label} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10, color: T.t3 }}>
            <span style={{ color, fontSize: 10 }}>{icon}</span> {label}
            {sub && <span style={{ fontSize: 9, color: T.t4 }}>{sub}</span>}
          </span>
        ))}
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   AS-IS LAYOUT — Dense Force Directed
   Shows the tangled mess of shared QMs. Nodes cluster naturally around
   heavily-shared QMs, visually communicating the complexity problem.
   ═══════════════════════════════════════════════════════════════════════════ */

function drawAsIsForce(g, nodes, edges, w, h, svg, zoom, title, showQueues) {
  const nodeCount = nodes.length;

  // Scale forces aggressively for large graphs
  const chargeStrength = nodeCount > 400 ? -60 : nodeCount > 200 ? -80 : nodeCount > 80 ? -120 : -250;
  const linkDist = nodeCount > 400 ? 12 : nodeCount > 200 ? 15 : nodeCount > 80 ? 30 : 40;
  const chDist = nodeCount > 400 ? 30 : nodeCount > 200 ? 40 : nodeCount > 80 ? 70 : 100;
  const ownsDist = 12;
  const collideRadius = nodeCount > 400 ? 6 : nodeCount > 200 ? 8 : nodeCount > 80 ? 15 : 22;

  // Pre-position nodes in a spread circle to avoid initial clumping
  // Without this, all nodes start at (0,0) and the simulation can't untangle them
  // in limited iterations, causing the bottom-left cluster problem
  const qmNodes_pre = nodes.filter(n => n.type === "qm");
  const appNodes_pre = nodes.filter(n => n.type === "app");
  const qmIdSet = new Set(qmNodes_pre.map(n => n.id));

  // Place QMs in a circle first
  qmNodes_pre.forEach((n, i) => {
    const angle = (2 * Math.PI * i) / qmNodes_pre.length;
    const radius = Math.min(w, h) * 0.3;
    n.x = w / 2 + radius * Math.cos(angle);
    n.y = h / 2 + radius * Math.sin(angle);
  });

  // Place apps near their connected QM (find via edges)
  const appQmMap = {};
  edges.forEach(e => {
    const src = typeof e.source === "object" ? e.source.id : e.source;
    const tgt = typeof e.target === "object" ? e.target.id : e.target;
    if (e.rel === "connects_to" && qmIdSet.has(tgt)) {
      appQmMap[src] = tgt;
    }
  });

  appNodes_pre.forEach(n => {
    const qmId = appQmMap[n.id];
    const qmNode = qmId ? qmNodes_pre.find(q => q.id === qmId) : null;
    if (qmNode) {
      // Place near the QM with some jitter
      n.x = qmNode.x + (Math.random() - 0.5) * 40;
      n.y = qmNode.y + (Math.random() - 0.5) * 40;
    } else {
      // No connected QM found — place randomly in center area
      n.x = w / 2 + (Math.random() - 0.5) * w * 0.5;
      n.y = h / 2 + (Math.random() - 0.5) * h * 0.5;
    }
  });

  const sim = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(edges).id(d => d.id)
      .distance(d => d.rel === "connects_to" ? linkDist : d.rel === "owns" ? ownsDist : chDist)
      .strength(d => d.rel === "connects_to" ? 1.2 : d.rel === "owns" ? 2.0 : 0.2))
    .force("charge", d3.forceManyBody().strength(chargeStrength))
    .force("center", d3.forceCenter(w / 2, h / 2))
    .force("collide", d3.forceCollide(collideRadius))
    .stop();

  // Run simulation synchronously — more iterations for large graphs to settle properly
  const iterations = nodeCount > 400 ? 300 : nodeCount > 200 ? 200 : 250;
  for (let i = 0; i < iterations; i++) sim.tick();

  // ── Draw edges ─────────────────────────────────────────────────────
  const visibleEdges = edges.filter(e => {
    if (showQueues) return true;
    return e.rel === "channel" || e.rel === "connects_to";
  });

  g.selectAll("line.edge").data(visibleEdges).join("line")
    .attr("class", "edge")
    .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
    .attr("x2", d => d.target.x).attr("y2", d => d.target.y)
    .attr("stroke", d => {
      if (d.rel === "channel") return `${T.cyan}35`;
      if (d.rel === "owns") return `${T.amber}25`;
      return `${T.green}20`;
    })
    .attr("stroke-width", d => d.rel === "channel" ? 1 : d.rel === "owns" ? 0.5 : 0.4)
    .attr("stroke-dasharray", d => d.rel === "connects_to" ? "2 2" : null);

  // ── Draw nodes ─────────────────────────────────────────────────────
  const qmNodes = nodes.filter(n => n.type === "qm");
  const appNodes = nodes.filter(n => n.type === "app");
  const queueNodes = nodes.filter(n => n.type === "queue");

  // Apps first (behind), then QMs on top
  // App dots — small, muted
  g.selectAll("circle.app-node").data(appNodes).join("circle")
    .attr("class", "app-node")
    .attr("cx", d => d.x).attr("cy", d => d.y)
    .attr("r", nodeCount > 200 ? 2.5 : 4)
    .attr("fill", `${T.green}60`)
    .attr("stroke", `${T.green}30`)
    .attr("stroke-width", 0.5);

  // Queue dots if shown
  if (showQueues && queueNodes.length) {
    g.selectAll("circle.queue-node").data(queueNodes).join("circle")
      .attr("class", "queue-node")
      .attr("cx", d => d.x).attr("cy", d => d.y)
      .attr("r", nodeCount > 200 ? 1.5 : 2.5)
      .attr("fill", `${T.amber}50`)
      .attr("stroke", "none");
  }

  // QM nodes — larger, prominent, with glow
  g.selectAll("circle.qm-glow").data(qmNodes).join("circle")
    .attr("class", "qm-glow")
    .attr("cx", d => d.x).attr("cy", d => d.y)
    .attr("r", nodeCount > 200 ? 8 : 14)
    .attr("fill", `${T.cyan}12`)
    .attr("stroke", "none");

  g.selectAll("circle.qm-node").data(qmNodes).join("circle")
    .attr("class", "qm-node")
    .attr("cx", d => d.x).attr("cy", d => d.y)
    .attr("r", nodeCount > 200 ? 5 : 10)
    .attr("fill", T.bg1)
    .attr("stroke", T.cyan)
    .attr("stroke-width", nodeCount > 200 ? 1 : 1.5);

  g.selectAll("text.qm-icon").data(qmNodes).join("text")
    .attr("class", "qm-icon")
    .attr("x", d => d.x).attr("y", d => d.y)
    .attr("text-anchor", "middle").attr("dominant-baseline", "central")
    .attr("fill", T.cyan)
    .attr("font-size", nodeCount > 200 ? "5px" : "8px")
    .text("◆");

  // Labels for QMs only (if not too crowded)
  if (nodeCount < 120) {
    g.selectAll("text.qm-label").data(qmNodes).join("text")
      .attr("class", "qm-label")
      .attr("x", d => d.x).attr("y", d => d.y + (nodeCount > 80 ? 14 : 18))
      .attr("text-anchor", "middle")
      .attr("fill", T.t4)
      .attr("font-size", "7px").attr("font-family", T.fontMono)
      .text(d => (d.name || d.id).replace("QM_", "").slice(0, 10));
  }

  // ── Zoom to fit ────────────────────────────────────────────────────
  zoomToFit(svg, zoom, nodes, w, h);
}


/* ═══════════════════════════════════════════════════════════════════════════
   TARGET LAYOUT — Radial Concentric Rings
   Each QM+App pair sits on concentric rings emanating from the center.
   Clean, ordered, instantly communicating "1 QM per app" architecture.
   Inter-QM channels drawn as subtle curved connections.
   ═══════════════════════════════════════════════════════════════════════════ */

function drawTargetRadial(g, nodes, edges, w, h, svg, zoom, title, showQueues) {
  const cx = w / 2;
  const cy = h / 2;

  // Separate node types
  const qmNodes = nodes.filter(n => n.type === "qm");
  const appNodes = nodes.filter(n => n.type === "app");
  const queueNodes = nodes.filter(n => n.type === "queue");

  // Build app→QM mapping from connects_to edges
  const appToQm = {};
  const qmToApps = {};
  const edgeId = e => typeof e.source === "object" ? e.source.id : e.source;
  const edgeTgt = e => typeof e.target === "object" ? e.target.id : e.target;

  edges.forEach(e => {
    if (e.rel === "connects_to") {
      const src = edgeId(e);
      const tgt = edgeTgt(e);
      const appNode = nodes.find(n => n.id === src && n.type === "app");
      const qmNode = nodes.find(n => n.id === tgt && n.type === "qm");
      if (appNode && qmNode) {
        appToQm[src] = tgt;
        if (!qmToApps[tgt]) qmToApps[tgt] = [];
        qmToApps[tgt].push(src);
      }
    }
  });

  // Build QM→queues mapping from owns edges (for showQueues mode)
  const qmToQueues = {};
  if (showQueues) {
    edges.forEach(e => {
      if (e.rel === "owns") {
        const src = edgeId(e);
        const tgt = edgeTgt(e);
        const queueNode = nodes.find(n => n.id === tgt && n.type === "queue");
        if (queueNode) {
          if (!qmToQueues[src]) qmToQueues[src] = [];
          qmToQueues[src].push(queueNode);
        }
      }
    });
  }

  // Build ordered list of QM-App pairs for ring placement
  // Also catch orphan apps (not connected to any QM)
  const pairs = [];
  const placedApps = new Set();

  qmNodes.forEach(qm => {
    const apps = qmToApps[qm.id] || [];
    if (apps.length > 0) {
      apps.forEach(appId => {
        pairs.push({ qm: qm, appId: appId, app: nodes.find(n => n.id === appId) });
        placedApps.add(appId);
      });
    } else {
      pairs.push({ qm: qm, appId: null, app: null });
    }
  });

  // Catch orphan apps (apps with no connects_to edge)
  appNodes.forEach(app => {
    if (!placedApps.has(app.id)) {
      pairs.push({ qm: null, appId: app.id, app: app });
    }
  });

  // Arrange pairs in concentric rings — dynamic spacing based on count
  const positions = [];
  let placed = 0;
  let ringIdx = 0;
  const totalPairs = pairs.length;
  // Adaptive sizing: more pairs = tighter packing
  const baseRadius = totalPairs > 120 ? Math.min(w, h) * 0.12 : Math.min(w, h) * 0.16;
  const ringSpacing = totalPairs > 120 ? Math.min(w, h) * 0.065 : Math.min(w, h) * 0.09;
  const minGap = totalPairs > 120 ? 14 : 20;

  while (placed < totalPairs) {
    const radius = baseRadius + ringIdx * ringSpacing;
    const circumference = 2 * Math.PI * radius;
    const maxInRing = Math.max(4, Math.floor(circumference / minGap));
    const count = Math.min(maxInRing, totalPairs - placed);

    for (let i = 0; i < count; i++) {
      const angle = (2 * Math.PI * i / count) - Math.PI / 2;
      const idx = placed + i;
      const pair = pairs[idx];
      const qmX = cx + Math.cos(angle) * radius;
      const qmY = cy + Math.sin(angle) * radius;
      const appDist = showQueues ? 16 : 12;
      const appX = cx + Math.cos(angle) * (radius + appDist);
      const appY = cy + Math.sin(angle) * (radius + appDist);

      positions.push({
        ...pair,
        qmX, qmY, appX, appY, angle, radius, idx
      });
    }
    placed += count;
    ringIdx++;
  }

  // Build position lookup by QM id (for channel drawing)
  const qmPositions = {};
  positions.forEach(p => {
    if (p.qm) qmPositions[p.qm.id] = { x: p.qmX, y: p.qmY };
  });

  // ── Ring guides (very subtle) ──────────────────────────────────────
  const rings = new Set(positions.map(p => p.radius));
  rings.forEach(r => {
    g.append("circle")
      .attr("cx", cx).attr("cy", cy).attr("r", r)
      .attr("fill", "none")
      .attr("stroke", `${T.border0}80`)
      .attr("stroke-width", 0.5)
      .attr("stroke-dasharray", "2 4");
  });

  // ── Inter-QM channels (curved connections) ─────────────────────────
  const channelEdges = edges.filter(e => e.rel === "channel");
  channelEdges.forEach(e => {
    const src = edgeId(e);
    const tgt = edgeTgt(e);
    const from = qmPositions[src];
    const to = qmPositions[tgt];
    if (from && to) {
      const midX = (from.x + to.x) / 2;
      const midY = (from.y + to.y) / 2;
      const pullFactor = 0.3;
      const ctrlX = midX + (cx - midX) * pullFactor;
      const ctrlY = midY + (cy - midY) * pullFactor;

      g.append("path")
        .attr("d", `M${from.x},${from.y} Q${ctrlX},${ctrlY} ${to.x},${to.y}`)
        .attr("fill", "none")
        .attr("stroke", `${T.cyan}25`)
        .attr("stroke-width", 0.6);
    }
  });

  // ── QM-App pair connections (short radial lines) ───────────────────
  positions.forEach(pos => {
    if (pos.app && pos.qm) {
      g.append("line")
        .attr("x1", pos.qmX).attr("y1", pos.qmY)
        .attr("x2", pos.appX).attr("y2", pos.appY)
        .attr("stroke", `${T.green}50`)
        .attr("stroke-width", 0.8);
    }
  });

  // ── App dots (outer ring of each pair) ─────────────────────────────
  positions.forEach(pos => {
    if (pos.app) {
      const isOrphan = !pos.qm;
      g.append("circle")
        .attr("cx", pos.appX).attr("cy", pos.appY)
        .attr("r", isOrphan ? 3 : 2.5)
        .attr("fill", isOrphan ? T.red : T.green)
        .attr("opacity", isOrphan ? 0.5 : 0.7);
    }
  });

  // ── QM dots (inner ring of each pair) ──────────────────────────────
  positions.forEach(pos => {
    if (!pos.qm) return;
    // Glow
    g.append("circle")
      .attr("cx", pos.qmX).attr("cy", pos.qmY)
      .attr("r", 6)
      .attr("fill", `${T.cyan}10`)
      .attr("stroke", "none");
    // Main dot
    g.append("circle")
      .attr("cx", pos.qmX).attr("cy", pos.qmY)
      .attr("r", 3.5)
      .attr("fill", T.bg1)
      .attr("stroke", T.cyan)
      .attr("stroke-width", 1);
    // Icon
    g.append("text")
      .attr("x", pos.qmX).attr("y", pos.qmY)
      .attr("text-anchor", "middle").attr("dominant-baseline", "central")
      .attr("fill", T.cyan)
      .attr("font-size", "4px")
      .text("◆");
  });

  // Center label removed — it obscured the channel web at scale.
  // The "1:1 dedicated" message is communicated by the ring structure itself
  // and the legend text "(dedicated 1:1)" below.

  // ── Zoom to fit all content ────────────────────────────────────────
  // Collect all rendered positions for zoom calculation
  const allPoints = [];
  positions.forEach(p => {
    allPoints.push({ x: p.qmX, y: p.qmY });
    if (p.app) allPoints.push({ x: p.appX, y: p.appY });
  });
  if (allPoints.length > 0) {
    zoomToFit(svg, zoom, allPoints, w, h);
  }
}


/* ═══════════════════════════════════════════════════════════════════════════
   TARGET FLOW LAYOUT — Filtered Single-App / Small Subgraph
   Shows the canonical MQ message flow:
   Producer App → QM_A → REMOTE_Q → XMITQ → Channel → QM_B → LOCAL_Q → Consumer App
   
   Used when ≤15 QMs are in the subgraph (i.e. app filter is active).
   This is what judges want to see — the actual message path.
   ═══════════════════════════════════════════════════════════════════════════ */

function drawTargetFlow(g, nodes, edges, w, h, svg, zoom, title, showQueues) {
  const edgeId = e => typeof e.source === "object" ? e.source.id : e.source;
  const edgeTgt = e => typeof e.target === "object" ? e.target.id : e.target;

  // Categorize nodes
  const qmNodes = nodes.filter(n => n.type === "qm");
  const appNodes = nodes.filter(n => n.type === "app");
  const queueNodes = nodes.filter(n => n.type === "queue");

  // Build mappings
  const appToQm = {};
  const qmToApps = {};
  edges.forEach(e => {
    if (e.rel === "connects_to") {
      const src = edgeId(e), tgt = edgeTgt(e);
      if (nodes.find(n => n.id === src && n.type === "app") && nodes.find(n => n.id === tgt && n.type === "qm")) {
        appToQm[src] = tgt;
        if (!qmToApps[tgt]) qmToApps[tgt] = [];
        qmToApps[tgt].push(src);
      }
    }
  });

  const qmToQueues = {};
  edges.forEach(e => {
    if (e.rel === "owns") {
      const src = edgeId(e), tgt = edgeTgt(e);
      const qNode = nodes.find(n => n.id === tgt && n.type === "queue");
      if (qNode) {
        if (!qmToQueues[src]) qmToQueues[src] = [];
        qmToQueues[src].push(qNode);
      }
    }
  });

  // Build channel pairs
  const channels = [];
  edges.forEach(e => {
    if (e.rel === "channel") {
      channels.push({ from: edgeId(e), to: edgeTgt(e), data: e });
    }
  });

  // ── Layout: vertical columns per QM ────────────────────────────────
  const qmCount = qmNodes.length;
  const isLargeFlow = qmCount > 15;
  const colWidth = isLargeFlow
    ? Math.max(80, Math.min(140, (w - 40) / Math.max(qmCount, 1)))
    : Math.max(120, Math.min(200, (w - 60) / Math.max(qmCount, 1)));
  const startX = isLargeFlow ? 20 : 40;
  const qmY = h * 0.35;
  const appY = h * 0.12;

  // Position each QM column
  const qmPos = {};
  qmNodes.forEach((qm, i) => {
    const x = startX + i * colWidth + colWidth / 2;
    qmPos[qm.id] = { x, y: qmY, idx: i };
  });

  // ── Draw channels between QMs (horizontal arrows) ──────────────────
  channels.forEach(ch => {
    const from = qmPos[ch.from];
    const to = qmPos[ch.to];
    if (!from || !to) return;

    const yOff = (Math.abs(from.idx - to.idx) > 1) ? -20 : 0;
    g.append("path")
      .attr("d", `M${from.x},${from.y + yOff} C${(from.x + to.x) / 2},${from.y + yOff - 30} ${(from.x + to.x) / 2},${to.y + yOff - 30} ${to.x},${to.y + yOff}`)
      .attr("fill", "none")
      .attr("stroke", T.cyan)
      .attr("stroke-width", 1.5)
      .attr("stroke-opacity", 0.4)
      .attr("marker-end", `url(#arrow-channel-${title})`);

    // Channel label
    const midX = (from.x + to.x) / 2;
    const midY = from.y + yOff - 18;
    const chName = ch.data.channel_name || `${ch.from}.${ch.to}`;
    g.append("text")
      .attr("x", midX).attr("y", midY)
      .attr("text-anchor", "middle")
      .attr("fill", `${T.cyan}80`)
      .attr("font-size", "7px").attr("font-family", T.fontMono)
      .text(chName.length > 20 ? chName.slice(0, 18) + "…" : chName);
  });

  // ── Message flow annotations (from REMOTE queue metadata) ──────────
  // Parse REMOTE queues to discover which apps talk to which apps
  // REMOTE queue nodes have: remote_qm, target_app, owner_app, source_queue
  if (showQueues) {
    const messageFlows = [];
    queueNodes.forEach(q => {
      if (q.queue_type === "REMOTE" && q.owner_app && q.target_app) {
        const existing = messageFlows.find(f => f.from === q.owner_app && f.to === q.target_app);
        if (!existing) {
          messageFlows.push({
            from: q.owner_app, to: q.target_app,
            queue: q.source_queue || q.name || q.id,
          });
        }
      }
    });

    // Draw flow arrows between apps if both are visible
    const flowY = appY - 30;
    messageFlows.forEach((flow, fi) => {
      const fromQm = appToQm[flow.from];
      const toQm = appToQm[flow.to];
      if (!fromQm || !toQm || !qmPos[fromQm] || !qmPos[toQm]) return;
      if (fromQm === toQm) return; // same QM, skip

      const fromX = qmPos[fromQm].x;
      const toX = qmPos[toQm].x;
      const yLine = flowY - (fi % 3) * 12;

      g.append("path")
        .attr("d", `M${fromX},${yLine} L${toX},${yLine}`)
        .attr("fill", "none")
        .attr("stroke", `${T.green}40`)
        .attr("stroke-width", 0.8)
        .attr("stroke-dasharray", "4 2")
        .attr("marker-end", `url(#arrow-connects_to-${title})`);

      // Flow label
      const qName = (flow.queue || "").replace("RQ.", "").slice(0, 18);
      if (qName) {
        g.append("text")
          .attr("x", (fromX + toX) / 2).attr("y", yLine - 4)
          .attr("text-anchor", "middle")
          .attr("fill", `${T.green}50`)
          .attr("font-size", "5.5px").attr("font-family", T.fontMono)
          .text(qName);
      }
    });
  }

  // ── Draw QM nodes ──────────────────────────────────────────────────
  qmNodes.forEach(qm => {
    const pos = qmPos[qm.id];
    if (!pos) return;

    // QM box
    const boxW = Math.min(100, colWidth - 20);
    g.append("rect")
      .attr("x", pos.x - boxW / 2).attr("y", pos.y - 18)
      .attr("width", boxW).attr("height", 36)
      .attr("rx", 6)
      .attr("fill", T.bg2)
      .attr("stroke", T.cyan)
      .attr("stroke-width", 1.5);

    // QM icon + label
    g.append("text")
      .attr("x", pos.x).attr("y", pos.y - 3)
      .attr("text-anchor", "middle")
      .attr("fill", T.cyan)
      .attr("font-size", "7px")
      .text("◆");
    g.append("text")
      .attr("x", pos.x).attr("y", pos.y + 10)
      .attr("text-anchor", "middle")
      .attr("fill", T.t2)
      .attr("font-size", "8px").attr("font-family", T.fontMono)
      .text((qm.name || qm.id).replace("QM_", "").slice(0, 14));
  });

  // ── Draw Apps above their QMs ──────────────────────────────────────
  Object.entries(appToQm).forEach(([appId, qmId]) => {
    const qPos = qmPos[qmId];
    if (!qPos) return;
    const appNode = nodes.find(n => n.id === appId);
    if (!appNode) return;

    const apps = qmToApps[qmId] || [];
    const appIdx = apps.indexOf(appId);
    const xOffset = apps.length > 1 ? (appIdx - (apps.length - 1) / 2) * 20 : 0;
    const ax = qPos.x + xOffset;
    const ay = appY;

    // App circle
    g.append("circle")
      .attr("cx", ax).attr("cy", ay).attr("r", 12)
      .attr("fill", T.bg2)
      .attr("stroke", T.green)
      .attr("stroke-width", 1.5);
    g.append("text")
      .attr("x", ax).attr("y", ay + 1)
      .attr("text-anchor", "middle").attr("dominant-baseline", "central")
      .attr("fill", T.green).attr("font-size", "7px")
      .text("●");

    // App label
    g.append("text")
      .attr("x", ax).attr("y", ay - 18)
      .attr("text-anchor", "middle")
      .attr("fill", T.t2)
      .attr("font-size", "8px").attr("font-family", T.fontMono)
      .text((appNode.name || appId).replace("Service_", "Svc_").slice(0, 14));

    // Connection line app → QM (server connection)
    g.append("line")
      .attr("x1", ax).attr("y1", ay + 12)
      .attr("x2", qPos.x).attr("y2", qPos.y - 18)
      .attr("stroke", `${T.green}60`)
      .attr("stroke-width", 1)
      .attr("stroke-dasharray", "3 3")
      .attr("marker-end", `url(#arrow-connects_to-${title})`);

    // Label the connection
    const connMidY = (ay + 12 + qPos.y - 18) / 2;
    g.append("text")
      .attr("x", (ax + qPos.x) / 2 + 8).attr("y", connMidY)
      .attr("fill", `${T.green}50`)
      .attr("font-size", "6px").attr("font-family", T.fontMono)
      .text("SVRCONN");
  });

  // ── Draw Queues below their QMs (when showQueues ON) ───────────────
  if (showQueues) {
    const queueStartY = qmY + 40;
    const queueRowH = 18;

    qmNodes.forEach(qm => {
      const pos = qmPos[qm.id];
      if (!pos) return;
      const queues = qmToQueues[qm.id] || [];
      if (queues.length === 0) return;

      // Group queues by type
      const localQs = queues.filter(q => q.queue_type !== "REMOTE" && q.usage !== "XMITQ" && !(q.name && q.name.includes("XMITQ")));
      const remoteQs = queues.filter(q => q.queue_type === "REMOTE");
      const xmitQs = queues.filter(q => q.usage === "XMITQ" || q.queue_type === "XMITQ" || (q.name && q.name.includes("XMITQ")));

      let rowIdx = 0;
      const drawQueueGroup = (qList, color, typeLabel) => {
        if (qList.length === 0) return;
        const y = queueStartY + rowIdx * queueRowH;
        // Type badge
        g.append("rect")
          .attr("x", pos.x - colWidth / 2 + 8).attr("y", y - 6)
          .attr("width", colWidth - 16).attr("height", 14)
          .attr("rx", 3)
          .attr("fill", `${color}10`)
          .attr("stroke", `${color}25`)
          .attr("stroke-width", 0.5);
        g.append("text")
          .attr("x", pos.x - colWidth / 2 + 14).attr("y", y + 3)
          .attr("fill", color)
          .attr("font-size", "7px").attr("font-family", T.fontMono)
          .text(`${typeLabel} (${qList.length})`);

        // Show first few queue names
        qList.slice(0, 2).forEach((q, qi) => {
          rowIdx++;
          const qy = queueStartY + rowIdx * queueRowH;
          g.append("text")
            .attr("x", pos.x - colWidth / 2 + 20).attr("y", qy + 3)
            .attr("fill", T.t4)
            .attr("font-size", "6px").attr("font-family", T.fontMono)
            .text((q.name || q.id).slice(0, 22));
        });
        if (qList.length > 2) {
          rowIdx++;
          const qy = queueStartY + rowIdx * queueRowH;
          g.append("text")
            .attr("x", pos.x - colWidth / 2 + 20).attr("y", qy + 3)
            .attr("fill", T.t4)
            .attr("font-size", "6px").attr("font-family", T.fontMono).attr("font-style", "italic")
            .text(`+${qList.length - 2} more`);
        }
        rowIdx++;
      };

      // Owns line from QM to queue area
      g.append("line")
        .attr("x1", pos.x).attr("y1", qmY + 18)
        .attr("x2", pos.x).attr("y2", queueStartY - 6)
        .attr("stroke", `${T.amber}30`)
        .attr("stroke-width", 0.5)
        .attr("stroke-dasharray", "2 2");

      drawQueueGroup(localQs, T.amber, "LOCAL");
      drawQueueGroup(remoteQs, T.amber, "REMOTE");
      drawQueueGroup(xmitQs, T.purple, "XMITQ");
    });
  }

  // ── Zoom to fit ────────────────────────────────────────────────────
  const allPoints = [];
  Object.values(qmPos).forEach(p => allPoints.push(p));
  appNodes.forEach(a => {
    const qmId = appToQm[a.id];
    if (qmId && qmPos[qmId]) allPoints.push({ x: qmPos[qmId].x, y: appY });
  });
  // Add bottom padding for queues
  if (showQueues && allPoints.length > 0) {
    allPoints.push({ x: allPoints[0].x, y: h * 0.9 });
  }
  if (allPoints.length > 0) zoomToFit(svg, zoom, allPoints, w, h);
}


/* ═══════════════════════════════════════════════════════════════════════════
   SHARED UTILITY — Zoom to Fit
   ═══════════════════════════════════════════════════════════════════════════ */

function zoomToFit(svg, zoom, nodes, w, h) {
  const xs = nodes.map(n => n.x).filter(v => isFinite(v));
  const ys = nodes.map(n => n.y).filter(v => isFinite(v));
  if (!xs.length) return;

  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(...ys), yMax = Math.max(...ys);
  const gw = xMax - xMin || 1, gh = yMax - yMin || 1;
  const pad = 40;
  // Cap scale at 2x to prevent over-zoom on tightly clustered graphs
  const scale = Math.min((w - pad * 2) / gw, (h - pad * 2) / gh, 2);
  const tx = (w - gw * scale) / 2 - xMin * scale;
  const ty = (h - gh * scale) / 2 - yMin * scale;

  svg.transition().duration(600).call(
    zoom.transform,
    d3.zoomIdentity.translate(tx, ty).scale(scale)
  );
}



/* ═══════════════════════════════════════════════════════════════════════════
   SCORE GAUGE — Radial ring
   ═══════════════════════════════════════════════════════════════════════════ */

function ScoreGauge({ label, score, max = 100, color, delay = 0 }) {
  const pct = Math.min((score / max) * 100, 100);
  const c = color || (score < 35 ? T.green : score < 65 ? T.amber : T.red);
  const r = 46;
  const circ = 2 * Math.PI * r;
  const offset = circ - (pct / 100) * circ;

  return (
    <div style={{ textAlign: "center", animation: `scaleIn 0.5s ease-out ${delay}s both` }}>
      <svg width="110" height="110" viewBox="0 0 110 110" style={{ display: "block", margin: "0 auto" }}>
        <circle cx="55" cy="55" r={r} fill="none" stroke={T.border0} strokeWidth="6" />
        <circle cx="55" cy="55" r={r} fill="none" stroke={c} strokeWidth="6"
          strokeDasharray={circ} strokeDashoffset={offset}
          strokeLinecap="round" transform="rotate(-90 55 55)"
          style={{ transition: "stroke-dashoffset 1s cubic-bezier(0.16, 1, 0.3, 1)" }}
        />
        <text x="55" y="52" textAnchor="middle" fill={c} fontSize="24" fontWeight="700" fontFamily={T.fontDisplay}>{score}</text>
        <text x="55" y="68" textAnchor="middle" fill={T.t3} fontSize="9" fontFamily={T.fontMono}>/100</text>
      </svg>
      <div style={{ fontSize: 11, color: T.t3, marginTop: 6, fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.06em" }}>{label}</div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   METRIC ROW
   ═══════════════════════════════════════════════════════════════════════════ */

function MetricRow({ label, before, after, delay = 0 }) {
  const improved = after < before;
  const delta = before - after;
  const pctChange = before > 0 ? Math.round((delta / before) * 100) : 0;
  return (
    <div style={{
      display: "grid", gridTemplateColumns: "1.5fr 80px 80px 100px",
      gap: 8, padding: "10px 16px", alignItems: "center",
      borderBottom: `1px solid ${T.border0}`,
      animation: `slideIn 0.3s ease-out ${delay}s both`,
      fontSize: 12,
    }}>
      <span style={{ color: T.t2, fontWeight: 500 }}>{label}</span>
      <span style={{ textAlign: "right", color: T.t3, fontFamily: T.fontMono }}>{before}</span>
      <span style={{ textAlign: "right", fontWeight: 600, fontFamily: T.fontMono, color: improved ? T.green : T.t1 }}>{after}</span>
      <span style={{ textAlign: "right", fontFamily: T.fontMono, fontSize: 11 }}>
        {improved
          ? <span style={{ color: T.green }}>↓ {delta} <span style={{ opacity: 0.7 }}>({pctChange}%)</span></span>
          : delta === 0 ? <span style={{ color: T.t4 }}>—</span> : <span style={{ color: T.red }}>↑ {Math.abs(delta)}</span>}
      </span>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   ADR CARD
   ═══════════════════════════════════════════════════════════════════════════ */

function ADRCard({ adr, delay = 0 }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{
      border: `1px solid ${open ? T.cyanBorder : T.border0}`,
      borderRadius: T.r2, overflow: "hidden",
      marginBottom: 8,
      animation: `fadeUp 0.3s ease-out ${delay}s both`,
      transition: "border-color 0.2s",
    }}>
      <div onClick={() => setOpen(!open)} style={{
        padding: "12px 16px", cursor: "pointer",
        display: "flex", justifyContent: "space-between", alignItems: "center",
        background: open ? `${T.bg3}80` : T.bg2,
        transition: "background 0.2s",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <Badge color={T.cyan} style={{ fontSize: 10 }}>{adr.id}</Badge>
          <span style={{ fontSize: 12, fontWeight: 500, color: T.t1 }}>{adr.decision || adr.title}</span>
        </div>
        <span style={{
          fontSize: 10, color: T.t3,
          transform: open ? "rotate(180deg)" : "rotate(0deg)",
          transition: "transform 0.2s",
        }}>▼</span>
      </div>
      {open && (
        <div style={{ padding: "14px 16px", fontSize: 12, color: T.t2, lineHeight: 1.7, borderTop: `1px solid ${T.border0}`, animation: "fadeIn 0.2s" }}>
          {[["Context", adr.context], ["Rationale", adr.rationale], ["Consequences", adr.consequences]].map(([k, v]) => v && (
            <div key={k} style={{ marginBottom: 10 }}>
              <span style={{ fontSize: 10, fontWeight: 600, color: T.t3, textTransform: "uppercase", letterSpacing: "0.06em" }}>{k}</span>
              <p style={{ margin: "4px 0 0", color: T.t2 }}>{v}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   VIOLATION BADGE
   ═══════════════════════════════════════════════════════════════════════════ */

function ViolationBadge({ v, delay = 0 }) {
  const isCrit = v.severity === "CRITICAL";
  const c = isCrit ? T.red : T.amber;
  return (
    <div style={{
      padding: "10px 14px", borderRadius: T.r1, marginBottom: 6,
      background: isCrit ? T.redBg : T.amberBg,
      border: `1px solid ${isCrit ? T.redBorder : T.amberBorder}`,
      display: "flex", alignItems: "flex-start", gap: 10,
      animation: `slideIn 0.3s ease-out ${delay}s both`,
      fontSize: 12,
    }}>
      <span style={{ fontSize: 10, fontWeight: 700, color: c, fontFamily: T.fontMono, flexShrink: 0, marginTop: 1 }}>{v.severity}</span>
      <span style={{ color: T.t2 }}><strong style={{ color: T.t1 }}>{v.rule}</strong> — {v.entity}: {v.detail}</span>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   PHASE BADGE for Migration Plan
   ═══════════════════════════════════════════════════════════════════════════ */

const PHASE_STYLES = {
  CREATE:  { color: T.cyan,   icon: "+" },
  REROUTE: { color: T.amber,  icon: "⇄" },
  DRAIN:   { color: T.purple, icon: "◎" },
  CLEANUP: { color: T.red,    icon: "×" },
};

function PhaseBadge({ phase }) {
  const s = PHASE_STYLES[phase] || { color: T.t3, icon: "?" };
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 4,
      padding: "2px 8px", borderRadius: 4,
      fontSize: 10, fontWeight: 600, fontFamily: T.fontMono,
      color: s.color, background: `${s.color}15`, border: `1px solid ${s.color}25`,
    }}>
      {s.icon} {phase}
    </span>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   LOADING SPINNER
   ═══════════════════════════════════════════════════════════════════════════ */

function LoadingOverlay({ liveSession }) {
  // If live session data is flowing, render the streaming agent feed.
  // Otherwise fall back to the rotating-message spinner.
  if (liveSession && liveSession.events && liveSession.events.length > 0) {
    return <LiveAgentStream liveSession={liveSession} />;
  }

  // Initial state — pipeline started but no events yet (very first 500ms)
  const [dots, setDots] = useState(0);
  useEffect(() => {
    const i = setInterval(() => setDots(d => (d + 1) % 4), 400);
    return () => clearInterval(i);
  }, []);

  return (
    <div style={{
      padding: "80px 20px", textAlign: "center",
      animation: "fadeIn 0.3s ease-out",
    }}>
      <div style={{
        width: 48, height: 48, margin: "0 auto 24px",
        border: `3px solid ${T.border1}`, borderTopColor: T.cyan,
        borderRadius: "50%", animation: "spin 0.8s linear infinite",
      }} />
      <div style={{ fontSize: 12, color: T.cyan, fontFamily: T.fontMono }}>
        Initializing pipeline{".".repeat(dots)}
      </div>
      <div style={{ fontSize: 11, color: T.t4, marginTop: 12 }}>
        {liveSession ? `Session ${liveSession.sessionId} starting...` : "Connecting to backend..."}
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   LIVE AGENT STREAM — renders agent activity as it arrives during pipeline
   ═══════════════════════════════════════════════════════════════════════════ */

// Color the agent name based on a stable hash of the agent name.
// Same agent always gets the same color across runs.
function _agentColor(name) {
  const palette = [T.cyan, T.green, T.purple, T.amber];
  let hash = 0;
  for (const ch of name) hash = ((hash << 5) - hash) + ch.charCodeAt(0);
  return palette[Math.abs(hash) % palette.length];
}

// Status icons for events. Currently we don't get distinct "started" vs "done"
// events from the backend — every log line is a single point-in-time event —
// so we render a uniform ◆ glyph and the LATEST event per agent gets a
// pulsing animation to suggest "this just happened."
function LiveAgentStream({ liveSession }) {
  const containerRef = useRef(null);
  const events = liveSession.events || [];
  const elapsed = liveSession.elapsedMs || 0;
  const elapsedSec = (elapsed / 1000).toFixed(1);
  const isDone = liveSession.status === "done";
  const isFailed = liveSession.status === "failed";

  // Auto-scroll to bottom on new events
  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [events.length]);

  // Group consecutive events from the same agent for a calmer feed.
  // Same agent emitting 3 events in a row → one stack with 3 sub-lines.
  const grouped = [];
  for (const ev of events) {
    const last = grouped[grouped.length - 1];
    if (last && last.agent === ev.agent) {
      last.events.push(ev);
    } else {
      grouped.push({ agent: ev.agent, events: [ev] });
    }
  }

  // Latest event sequence (so we can pulse the most recent line)
  const latestSeq = events.length > 0 ? events[events.length - 1].sequence : -1;

  return (
    <div style={{
      padding: "32px 20px",
      maxWidth: 920, margin: "0 auto",
      animation: "fadeIn 0.3s ease-out",
    }}>
      {/* HEADER */}
      <div style={{ marginBottom: 20, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1, marginBottom: 2 }}>
            {isDone   ? "✓ Pipeline Complete"
             : isFailed ? "✗ Pipeline Failed"
             : "Pipeline Running"}
          </div>
          <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono }}>
            session={liveSession.sessionId} · {events.length} events · {elapsedSec}s elapsed
          </div>
        </div>
        {!isDone && !isFailed && (
          <div style={{
            display: "flex", alignItems: "center", gap: 8,
            padding: "6px 14px", borderRadius: 20,
            background: T.cyanBg, border: `1px solid ${T.cyanBorder}`,
            fontSize: 11, color: T.cyan, fontFamily: T.fontMono, fontWeight: 600,
          }}>
            <span style={{
              width: 8, height: 8, borderRadius: "50%",
              background: T.cyan, animation: "pulse 1.4s ease-in-out infinite",
            }} />
            LIVE
          </div>
        )}
        {isDone && (
          <div style={{
            padding: "6px 14px", borderRadius: 20,
            background: T.greenBg, border: `1px solid ${T.greenBorder}`,
            fontSize: 11, color: T.green, fontFamily: T.fontMono, fontWeight: 600,
          }}>
            ✓ DONE
          </div>
        )}
        {isFailed && (
          <div style={{
            padding: "6px 14px", borderRadius: 20,
            background: T.redBg, border: `1px solid ${T.redBorder}`,
            fontSize: 11, color: T.red, fontFamily: T.fontMono, fontWeight: 600,
          }}>
            ✗ FAILED
          </div>
        )}
      </div>

      {/* TIMELINE */}
      <div
        ref={containerRef}
        style={{
          background: T.bg2,
          border: `1px solid ${T.border0}`,
          borderRadius: T.r2,
          padding: "16px 20px",
          maxHeight: 480, overflowY: "auto",
          fontFamily: T.fontMono,
          fontSize: 12,
          position: "relative",
        }}
      >
        {grouped.length === 0 ? (
          <div style={{ color: T.t3, textAlign: "center", padding: 32 }}>
            Waiting for first event...
          </div>
        ) : (
          <div style={{ position: "relative" }}>
            {/* Timeline rail */}
            <div style={{
              position: "absolute", left: 96, top: 8, bottom: 8,
              width: 1, background: T.border0,
            }} />
            {grouped.map((group, gi) => {
              const c = _agentColor(group.agent);
              return (
                <div key={gi} style={{
                  display: "flex", gap: 12, padding: "6px 0",
                  alignItems: "flex-start",
                  animation: `slideIn 0.25s ease-out`,
                }}>
                  <span style={{
                    fontSize: 10, fontWeight: 600,
                    color: c, minWidth: 84, textAlign: "right",
                    paddingTop: 4, flexShrink: 0,
                    letterSpacing: "0.04em",
                  }}>{group.agent}</span>
                  <div style={{
                    width: 9, height: 9, borderRadius: "50%",
                    background: c, border: `2px solid ${T.bg2}`,
                    marginTop: 6, flexShrink: 0, zIndex: 1,
                    boxShadow: group.events.some(e => e.sequence === latestSeq && !isDone && !isFailed)
                      ? `0 0 0 4px ${c}30` : "none",
                  }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    {group.events.map((e, ei) => {
                      const isLatest = e.sequence === latestSeq && !isDone && !isFailed;
                      const isFinal = ei === group.events.length - 1;
                      return (
                        <div key={e.sequence} style={{
                          fontSize: 12, color: T.t2, lineHeight: 1.5,
                          padding: isFinal ? "4px 12px" : "2px 12px",
                          marginBottom: isFinal ? 4 : 0,
                          background: isLatest ? T.bg3 : "transparent",
                          borderRadius: T.r1,
                          transition: "background 0.2s",
                          wordBreak: "break-word",
                        }}>
                          {e.message}
                          <span style={{
                            marginLeft: 8, fontSize: 10, color: T.t4,
                          }}>+{(e.elapsed_ms / 1000).toFixed(2)}s</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })}
            {/* Active processing indicator */}
            {!isDone && !isFailed && (
              <div style={{
                display: "flex", gap: 12, padding: "8px 0", marginLeft: 96,
                color: T.t3, fontSize: 11,
              }}>
                <span style={{ marginLeft: 24, fontStyle: "italic" }}>
                  <span style={{ display: "inline-block", animation: "pulse 1s infinite" }}>◆</span>
                  {" "}working...
                </span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* FAILURE PANEL */}
      {isFailed && liveSession.error && (
        <div style={{
          marginTop: 16, padding: "12px 16px",
          background: T.redBg, border: `1px solid ${T.redBorder}`,
          borderRadius: T.r1, color: T.red, fontSize: 12,
          fontFamily: T.fontMono,
        }}>
          <div style={{ fontWeight: 600, marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em", fontSize: 10 }}>
            Pipeline Error
          </div>
          {liveSession.error}
        </div>
      )}

      {/* COMPLETION PANEL */}
      {isDone && (
        <div style={{
          marginTop: 16, padding: "12px 16px",
          background: T.greenBg, border: `1px solid ${T.greenBorder}`,
          borderRadius: T.r1, color: T.green, fontSize: 12,
          fontFamily: T.fontMono, textAlign: "center",
        }}>
          ✓ Completed in {elapsedSec}s — fetching results...
        </div>
      )}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   TABS CONFIGURATION
   ═══════════════════════════════════════════════════════════════════════════ */

const TAB_CONFIG = [
  { id: "upload",     label: "Upload",     icon: "⬆" },
  { id: "review",     label: "Review",     icon: "◎" },
  { id: "topology",   label: "Topology",   icon: "◇" },
  { id: "solver",     label: "Solver",     icon: "Σ" },
  { id: "compliance", label: "Compliance", icon: "✓" },
  { id: "metrics",    label: "Metrics",    icon: "▤" },
  { id: "adrs",       label: "ADRs",       icon: "◈" },
  { id: "migration",  label: "Migration",  icon: "⇄" },
  { id: "mqsc",       label: "MQSC",       icon: "▸" },
  { id: "csvs",       label: "CSVs",       icon: "⊞" },
  { id: "report",     label: "Report",     icon: "◫" },
  { id: "trace",      label: "Trace",      icon: "⋯" },
];


/* ═══════════════════════════════════════════════════════════════════════════
   MAIN APP
   ═══════════════════════════════════════════════════════════════════════════ */

// ═══════════════════════════════════════════════════════════════════════════
// DIFF TOPOLOGY VIEW — overlay showing added/removed/unchanged elements
// Layout: original QMs on inner ring, new QMs on outer ring
// Z-order: unchanged (back) → added (mid) → removed (front, prominent)
// ═══════════════════════════════════════════════════════════════════════════
function DiffTopologyView({ asIsGraph, targetGraph, diff, height = 600 }) {
  const svgRef = useRef(null);

  useEffect(() => {
    if (!asIsGraph || !targetGraph || !diff || !svgRef.current) return;

    const svg = svgRef.current;
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    const w = svg.clientWidth || 900;
    const h = height;

    // Classify nodes
    const asIsNodes = new Set((asIsGraph.nodes || []).filter(n => n.type === "qm").map(n => n.id));
    const targetNodes = new Set((targetGraph.nodes || []).filter(n => n.type === "qm").map(n => n.id));
    const removedQMs = new Set(diff.qms_removed || []);
    const addedQMs = new Set(diff.qms_added || []);
    const unchangedQMs = [...asIsNodes].filter(q => targetNodes.has(q) && !removedQMs.has(q) && !addedQMs.has(q));

    // Classify edges
    const addedChannels = new Set((diff.channels_added || []).map(c => `${c[0]}→${c[1]}`));
    const removedChannels = new Set((diff.channels_removed || []).map(c => `${c[0]}→${c[1]}`));
    const asIsEdgeKeys = new Set((asIsGraph.edges || []).filter(e => e.rel === "channel").map(e => `${e.source}→${e.target}`));
    const targetEdgeKeys = new Set((targetGraph.edges || []).filter(e => e.rel === "channel").map(e => `${e.source}→${e.target}`));
    const unchangedEdges = [...asIsEdgeKeys].filter(k => targetEdgeKeys.has(k) && !removedChannels.has(k) && !addedChannels.has(k));

    const allQMs = [...new Set([...asIsNodes, ...targetNodes])];
    if (allQMs.length === 0) return;

    const cx = w / 2, cy = h / 2;
    const mkEl = (tag) => svg.ownerDocument.createElementNS(svg.namespaceURI, tag);

    // ── DUAL RING LAYOUT ──
    // Inner ring: original/unchanged QMs (the as-is backbone)
    // Outer ring: newly added QMs (the 1:1 expansions)
    // Removed QMs placed on inner ring with X marker
    const innerQMs = [...unchangedQMs, ...[...removedQMs]];
    const outerQMs = [...addedQMs];
    const innerR = Math.min(w, h) * 0.24;
    const outerR = Math.min(w, h) * 0.42;
    const positions = {};

    innerQMs.forEach((qm, i) => {
      const angle = (2 * Math.PI * i) / Math.max(innerQMs.length, 1) - Math.PI / 2;
      positions[qm] = { x: cx + innerR * Math.cos(angle), y: cy + innerR * Math.sin(angle) };
    });
    outerQMs.forEach((qm, i) => {
      const angle = (2 * Math.PI * i) / Math.max(outerQMs.length, 1) - Math.PI / 2;
      positions[qm] = { x: cx + outerR * Math.cos(angle), y: cy + outerR * Math.sin(angle) };
    });

    // ── LAYER 0: Ring guides (subtle) ──
    [innerR, outerR].forEach(r => {
      const circle = mkEl("circle");
      circle.setAttribute("cx", cx); circle.setAttribute("cy", cy); circle.setAttribute("r", r);
      circle.setAttribute("fill", "none"); circle.setAttribute("stroke", "#1e293b");
      circle.setAttribute("stroke-width", "1"); circle.setAttribute("stroke-dasharray", "3,6");
      svg.appendChild(circle);
    });

    // Ring labels
    const labelInner = mkEl("text");
    labelInner.setAttribute("x", cx); labelInner.setAttribute("y", cy - innerR - 8);
    labelInner.setAttribute("text-anchor", "middle"); labelInner.setAttribute("font-size", "9");
    labelInner.setAttribute("font-family", "DM Sans, sans-serif"); labelInner.setAttribute("fill", "#475569");
    labelInner.textContent = `ORIGINAL QMs (${innerQMs.length})`;
    svg.appendChild(labelInner);

    const labelOuter = mkEl("text");
    labelOuter.setAttribute("x", cx); labelOuter.setAttribute("y", cy - outerR - 8);
    labelOuter.setAttribute("text-anchor", "middle"); labelOuter.setAttribute("font-size", "9");
    labelOuter.setAttribute("font-family", "DM Sans, sans-serif"); labelOuter.setAttribute("fill", "#22c55e60");
    labelOuter.textContent = `NEW DEDICATED QMs (${outerQMs.length})`;
    svg.appendChild(labelOuter);

    // Helper to draw a line
    function drawLine(src, tgt, color, width, opacity, dash) {
      if (!positions[src] || !positions[tgt]) return null;
      const line = mkEl("line");
      line.setAttribute("x1", positions[src].x); line.setAttribute("y1", positions[src].y);
      line.setAttribute("x2", positions[tgt].x); line.setAttribute("y2", positions[tgt].y);
      line.setAttribute("stroke", color); line.setAttribute("stroke-width", width);
      line.setAttribute("opacity", opacity);
      if (dash) line.setAttribute("stroke-dasharray", dash);
      return line;
    }

    // ── LAYER 1: Unchanged edges (very faint, background) ──
    unchangedEdges.forEach(key => {
      const [src, tgt] = key.split("→");
      const line = drawLine(src, tgt, "#334155", "0.5", "0.2", null);
      if (line) svg.appendChild(line);
    });

    // ── LAYER 2: Added edges (green, moderate) ──
    addedChannels.forEach(key => {
      const [src, tgt] = key.split("→");
      const line = drawLine(src, tgt, "#22c55e", "1.2", "0.35", null);
      if (line) svg.appendChild(line);
    });

    // ── LAYER 3: Removed edges (red, prominent, ON TOP) ──
    removedChannels.forEach(key => {
      const [src, tgt] = key.split("→");
      const line = drawLine(src, tgt, "#ef4444", "1.5", "0.7", "4,3");
      if (line) svg.appendChild(line);
    });

    // ── LAYER 4: Nodes (always on top of edges) ──
    const nodeSize = allQMs.length > 80 ? 5 : allQMs.length > 40 ? 7 : 10;
    const fontSize = allQMs.length > 80 ? 5 : allQMs.length > 40 ? 6.5 : 8;
    const showLabels = allQMs.length <= 100;

    // Unchanged nodes
    unchangedQMs.forEach(qm => {
      const pos = positions[qm]; if (!pos) return;
      const c = mkEl("circle")
      c.setAttribute("cx", pos.x); c.setAttribute("cy", pos.y);
      c.setAttribute("r", nodeSize * 0.8); c.setAttribute("fill", "#475569"); c.setAttribute("opacity", "0.6");
      svg.appendChild(c);
      if (showLabels) {
        const t = mkEl("text");
        t.setAttribute("x", pos.x); t.setAttribute("y", pos.y + nodeSize + 8);
        t.setAttribute("text-anchor", "middle"); t.setAttribute("font-size", fontSize);
        t.setAttribute("font-family", "JetBrains Mono, monospace"); t.setAttribute("fill", "#64748b");
        t.textContent = qm.length > 10 ? qm.slice(0, 9) + "…" : qm;
        svg.appendChild(t);
      }
    });

    // Added nodes (green, outer ring)
    outerQMs.forEach(qm => {
      const pos = positions[qm]; if (!pos) return;
      const c = mkEl("circle");
      c.setAttribute("cx", pos.x); c.setAttribute("cy", pos.y);
      c.setAttribute("r", nodeSize); c.setAttribute("fill", "#22c55e"); c.setAttribute("opacity", "0.8");
      svg.appendChild(c);
      if (showLabels) {
        const t = mkEl("text");
        t.setAttribute("x", pos.x); t.setAttribute("y", pos.y + nodeSize + 8);
        t.setAttribute("text-anchor", "middle"); t.setAttribute("font-size", fontSize);
        t.setAttribute("font-family", "JetBrains Mono, monospace"); t.setAttribute("fill", "#22c55e");
        t.textContent = qm.length > 10 ? qm.slice(0, 9) + "…" : qm;
        svg.appendChild(t);
      }
    });

    // Removed nodes (red X marker, very prominent)
    [...removedQMs].forEach(qm => {
      const pos = positions[qm]; if (!pos) return;
      const s = nodeSize * 1.2;
      // Red circle with X
      const c = mkEl("circle");
      c.setAttribute("cx", pos.x); c.setAttribute("cy", pos.y);
      c.setAttribute("r", s); c.setAttribute("fill", "#ef444430");
      c.setAttribute("stroke", "#ef4444"); c.setAttribute("stroke-width", "2");
      svg.appendChild(c);
      // X cross
      const x1 = mkEl("line");
      x1.setAttribute("x1", pos.x - s * 0.5); x1.setAttribute("y1", pos.y - s * 0.5);
      x1.setAttribute("x2", pos.x + s * 0.5); x1.setAttribute("y2", pos.y + s * 0.5);
      x1.setAttribute("stroke", "#ef4444"); x1.setAttribute("stroke-width", "2");
      svg.appendChild(x1);
      const x2 = mkEl("line");
      x2.setAttribute("x1", pos.x + s * 0.5); x2.setAttribute("y1", pos.y - s * 0.5);
      x2.setAttribute("x2", pos.x - s * 0.5); x2.setAttribute("y2", pos.y + s * 0.5);
      x2.setAttribute("stroke", "#ef4444"); x2.setAttribute("stroke-width", "2");
      svg.appendChild(x2);
      if (showLabels) {
        const t = mkEl("text");
        t.setAttribute("x", pos.x); t.setAttribute("y", pos.y + s + 10);
        t.setAttribute("text-anchor", "middle"); t.setAttribute("font-size", fontSize);
        t.setAttribute("font-family", "JetBrains Mono, monospace"); t.setAttribute("fill", "#ef4444");
        t.textContent = qm;
        svg.appendChild(t);
      }
    });

    // ── LAYER 5: Legend ──
    const legendBg = mkEl("rect");
    legendBg.setAttribute("x", 10); legendBg.setAttribute("y", 10);
    legendBg.setAttribute("width", 175); legendBg.setAttribute("height", 105);
    legendBg.setAttribute("rx", 6); legendBg.setAttribute("fill", "#0f172aDD");
    legendBg.setAttribute("stroke", "#1e293b");
    svg.appendChild(legendBg);

    const legend = [
      { color: "#ef4444", dash: "4,3", width: 2.5, label: `Removed (${removedChannels.size} ch, ${removedQMs.size} QMs)` },
      { color: "#22c55e", dash: null, width: 2, label: `Added (${addedChannels.size} ch, ${addedQMs.size} QMs)` },
      { color: "#475569", dash: null, width: 1, label: `Unchanged (${unchangedEdges.length} ch)` },
    ];
    legend.forEach((item, i) => {
      const y = 30 + i * 24;
      const line = mkEl("line");
      line.setAttribute("x1", 20); line.setAttribute("y1", y);
      line.setAttribute("x2", 44); line.setAttribute("y2", y);
      line.setAttribute("stroke", item.color); line.setAttribute("stroke-width", item.width);
      if (item.dash) line.setAttribute("stroke-dasharray", item.dash);
      svg.appendChild(line);

      const text = mkEl("text");
      text.setAttribute("x", 50); text.setAttribute("y", y + 4);
      text.setAttribute("font-size", "10"); text.setAttribute("font-family", "DM Sans, sans-serif");
      text.setAttribute("fill", item.color);
      text.textContent = item.label;
      svg.appendChild(text);
    });

    // Net reduction callout (bottom right)
    const netCh = removedChannels.size - addedChannels.size;
    if (netCh > 0) {
      const callout = mkEl("text");
      callout.setAttribute("x", w - 20); callout.setAttribute("y", h - 20);
      callout.setAttribute("text-anchor", "end"); callout.setAttribute("font-size", "14");
      callout.setAttribute("font-family", "JetBrains Mono, monospace");
      callout.setAttribute("font-weight", "700"); callout.setAttribute("fill", "#22c55e");
      callout.textContent = `NET: -${netCh} channels`;
      svg.appendChild(callout);
    }

  }, [asIsGraph, targetGraph, diff, height]);

  const diffStats = diff || {};
  const netChannels = (diffStats.channels_removed?.length || 0) - (diffStats.channels_added?.length || 0);
  return (
    <div style={{
      background: T.bg1, border: `1px solid ${T.border1}`, borderRadius: T.r2,
      marginBottom: 16, overflow: "hidden",
    }}>
      <div style={{
        padding: "10px 16px", borderBottom: `1px solid ${T.border1}`,
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: 14 }}>◈</span>
          <span style={{ fontFamily: T.fontMono, fontSize: 12, fontWeight: 600, color: T.t1 }}>
            Topology Diff — As-Is vs Target
          </span>
          {netChannels > 0 && (
            <Badge color="#22c55e" style={{ fontSize: 10, fontWeight: 700 }}>
              NET: -{netChannels} channels simplified
            </Badge>
          )}
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <Badge color="#22c55e" style={{ fontSize: 9 }}>+{(diffStats.qms_added?.length || 0)} QMs</Badge>
          <Badge color="#ef4444" style={{ fontSize: 9 }}>-{(diffStats.qms_removed?.length || 0)} QMs</Badge>
          <Badge color="#22c55e" style={{ fontSize: 9 }}>+{(diffStats.channels_added?.length || 0)} ch</Badge>
          <Badge color="#ef4444" style={{ fontSize: 9 }}>-{(diffStats.channels_removed?.length || 0)} ch</Badge>
          <Badge color={T.amber} style={{ fontSize: 9 }}>{(diffStats.apps_reassigned?.length || 0)} moved</Badge>
        </div>
      </div>
      <svg ref={svgRef} width="100%" height={height} style={{ display: "block" }} />
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState("upload");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [reviewFeedback, setReviewFeedback] = useState("");
  const [reviewLoading, setReviewLoading] = useState(false);
  const [topoShowQueues, setTopoShowQueues] = useState(false);
  const [topoFilterApp, setTopoFilterApp] = useState("");
  const [topoViewMode, setTopoViewMode] = useState("split"); // "split" | "diff"

  // ── Live Agent Activity Stream (A3) ────────────────────────────────────
  // While a pipeline runs, we poll /api/session/{id}/progress every 500ms.
  // Events stream in and are rendered by <LiveAgentStream> during loading.
  const [liveSession, setLiveSession] = useState(null);
  // liveSession shape:
  //   { sessionId, events: [...], status: "running"|"done"|"failed",
  //     elapsedMs, error, startedAt }

  // ── LOCAL approval tracking ──
  // The backend pipeline re-runs from start on resume (Known Limitation #1).
  // This can reset human_approved to null and awaiting_human_review to true.
  // So we track the user's decision locally — this is the SOURCE OF TRUTH.
  const [userDecision, setUserDecision] = useState(null); // null | "approved" | "rejected" | "aborted"

  // Inject styles once
  useEffect(() => {
    const existing = document.getElementById("intelliai-styles");
    if (!existing) {
      const el = document.createElement("style");
      el.id = "intelliai-styles";
      el.textContent = STYLE_TAG;
      document.head.appendChild(el);
    }
  }, []);

  const architectMethod = result?.architect_method;

  // ── App list for topology filter dropdown ────────────────────────────
  const appList = useMemo(() => {
    if (!result?.target_graph?.nodes) return [];
    return result.target_graph.nodes
      .filter(n => n.type === "app")
      .map(n => ({ id: n.id, name: n.name || n.id }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [result?.target_graph]);

  // ── Filtered graph for app path tracing ──────────────────────────────
  const filteredTargetGraph = useMemo(() => {
    if (!topoFilterApp || !result?.target_graph) return result?.target_graph;
    const g = result.target_graph;
    // Find the app's QM
    const appEdge = g.edges.find(e =>
      (e.source === topoFilterApp || e.source?.id === topoFilterApp) && e.rel === "connects_to"
    );
    if (!appEdge) return g;
    const appQm = typeof appEdge.target === "string" ? appEdge.target : appEdge.target?.id;
    
    // Find all channels from/to this QM
    const connectedQMs = new Set([appQm]);
    const relevantChannels = g.edges.filter(e => {
      if (e.rel !== "channel") return false;
      const src = typeof e.source === "string" ? e.source : e.source?.id;
      const tgt = typeof e.target === "string" ? e.target : e.target?.id;
      if (src === appQm || tgt === appQm) {
        connectedQMs.add(src);
        connectedQMs.add(tgt);
        return true;
      }
      return false;
    });

    // Find all apps on connected QMs
    const relevantApps = new Set();
    g.edges.forEach(e => {
      if (e.rel !== "connects_to") return;
      const src = typeof e.source === "string" ? e.source : e.source?.id;
      const tgt = typeof e.target === "string" ? e.target : e.target?.id;
      if (connectedQMs.has(tgt)) relevantApps.add(src);
    });

    // Find all queues owned by connected QMs
    const relevantQueues = new Set();
    g.edges.forEach(e => {
      if (e.rel !== "owns") return;
      const src = typeof e.source === "string" ? e.source : e.source?.id;
      const tgt = typeof e.target === "string" ? e.target : e.target?.id;
      if (connectedQMs.has(src)) relevantQueues.add(tgt);
    });

    const keepNodes = new Set([...connectedQMs, ...relevantApps, ...relevantQueues]);
    return {
      nodes: g.nodes.filter(n => keepNodes.has(n.id)),
      edges: g.edges.filter(e => {
        const src = typeof e.source === "string" ? e.source : e.source?.id;
        const tgt = typeof e.target === "string" ? e.target : e.target?.id;
        return keepNodes.has(src) && keepNodes.has(tgt);
      }),
    };
  }, [result?.target_graph, topoFilterApp]);

  // ── Derived state flags — LOCAL userDecision is the source of truth ──
  // The backend may return contradictory flags because the pipeline re-runs
  // from start, so we never trust awaiting_human_review or human_approved
  // from the response alone.
  const isApproved = userDecision === "approved";
  const isAborted = userDecision === "aborted";
  const isRejected = userDecision === "rejected";
  const isAwaitingReview = result && !isApproved && !isAborted && !isRejected;

  // Check if post-approval data actually exists in the response
  const hasOutputs = !!(
    (result?.mqsc_scripts?.length) ||
    (result?.target_csvs && Object.keys(result.target_csvs).length > 0) ||
    result?.migration_plan ||
    result?.final_report
  );

  async function submitReview(approved, abort = false, feedbackOverride = null, chatHistory = null) {
    const feedback = feedbackOverride !== null ? feedbackOverride : reviewFeedback;
    if (!approved && !abort && !feedback.trim()) {
      setError("Please provide a reason when revising — the Architect needs your feedback to redesign.");
      return;
    }
    // Track the decision locally BEFORE the API call
    if (approved) setUserDecision("approved");
    else if (abort) setUserDecision("aborted");
    else setUserDecision("rejected");

    // Show full loading spinner — pipeline re-runs and takes 10-30s
    setLoading(true);
    setReviewLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}/api/review/${result?.session_id}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ approved, feedback, abort, chat_history: chatHistory }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();

      console.log("[IntelliAI] Review response keys:", Object.keys(data));
      console.log("[IntelliAI] Review response flags:", {
        human_approved: data.human_approved,
        awaiting_human_review: data.awaiting_human_review,
        has_mqsc: Array.isArray(data.mqsc_scripts) && data.mqsc_scripts.length > 0,
        has_csvs: data.target_csvs && Object.keys(data.target_csvs).length > 0,
        has_migration: !!data.migration_plan,
        has_report: !!data.final_report,
      });

      setResult(data);
      setReviewFeedback("");

      if (approved) setTab("topology");
      else if (abort) setTab("trace");
      else {
        // Revision complete — reset decision so review panel shows the new design
        setUserDecision(null);
      }
    } catch (e) {
      // Roll back the local decision on error
      setUserDecision(null);
      setError(e.message);
    } finally {
      setLoading(false);
      setReviewLoading(false);
    }
  }

  // Helper that drives the full async pipeline lifecycle:
  // POST → start polling → render events live → fetch result on done.
  // Returns a Promise that resolves when the pipeline completes (or rejects).
  async function runPipelineAsync(launchFn) {
    setLoading(true);
    setError(null);
    setUserDecision(null);
    setResult(null);

    const launchResp = await launchFn();
    if (!launchResp.ok) {
      const errText = await launchResp.text();
      setLoading(false);
      throw new Error(errText || `HTTP ${launchResp.status}`);
    }
    const launchData = await launchResp.json();
    const sessionId = launchData.session_id;
    if (!sessionId) {
      setLoading(false);
      throw new Error("No session_id returned from launch endpoint");
    }

    setLiveSession({
      sessionId,
      events: [],
      status: "running",
      elapsedMs: 0,
      error: null,
      startedAt: Date.now(),
    });

    // Poll loop. Polls every 500ms; updates liveSession state as events arrive.
    // Stops when status="done" or status="failed".
    let lastSeq = 0;
    let polling = true;
    let finalStatus = null;

    while (polling) {
      await new Promise(r => setTimeout(r, 500));
      try {
        const r = await fetch(`${API}/api/session/${sessionId}/progress?since_seq=${lastSeq}`);
        if (!r.ok) {
          // 404 transient on very first poll if backend just spun up — retry
          if (r.status === 404) continue;
          throw new Error(`Progress poll HTTP ${r.status}`);
        }
        const p = await r.json();
        if (p.events && p.events.length > 0) {
          lastSeq = Math.max(lastSeq, ...p.events.map(e => e.sequence));
          setLiveSession(prev => prev ? {
            ...prev,
            events: [...prev.events, ...p.events],
            status: p.status,
            elapsedMs: p.elapsed_ms,
            error: p.error,
          } : prev);
        } else {
          setLiveSession(prev => prev ? {
            ...prev,
            status: p.status,
            elapsedMs: p.elapsed_ms,
            error: p.error,
          } : prev);
        }
        if (p.status === "done" || p.status === "failed") {
          finalStatus = p.status;
          polling = false;
        }
      } catch (e) {
        // Transient network blip — wait and retry
        console.warn("[IntelliAI] progress poll error:", e.message);
      }
    }

    if (finalStatus === "failed") {
      const errMsg = liveSession?.error || "Pipeline failed";
      setError(errMsg);
      setLoading(false);
      return null;
    }

    // Pipeline done — fetch the full result
    try {
      const r = await fetch(`${API}/api/session/${sessionId}`);
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      console.log("[IntelliAI] Pipeline result keys:", Object.keys(data));
      setResult(data);
      setTab("review");
      return data;
    } finally {
      setLoading(false);
      // Keep liveSession around briefly so the user can see "Done in N seconds"
      // before it disappears. Comment this out to keep it visible always.
      setTimeout(() => setLiveSession(null), 3000);
    }
  }

  async function runDemo() {
    try {
      await runPipelineAsync(() => fetch(`${API}/api/demo`, { method: "POST" }));
    } catch (e) {
      setError(e.message);
      setLoading(false);
    }
  }

  async function handleUpload(e) {
    e.preventDefault();
    const fileInput = e.target.querySelector('input[name="mq_raw_data"]');
    if (!fileInput?.files?.length) {
      setError("Please select an MQ Raw Data CSV file");
      return;
    }
    const form = new FormData();
    form.append("file", fileInput.files[0]);
    try {
      await runPipelineAsync(() => fetch(`${API}/api/upload`, { method: "POST", body: form }));
    } catch (e) {
      setError(e.message);
      setLoading(false);
    }
  }

  function downloadFile(content, filename, mime = "text/plain") {
    const blob = new Blob([content], { type: mime });
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
    a.download = filename; a.click();
  }

  return (
    <div style={{ minHeight: "100vh", background: T.bg0 }}>
      {/* ── HEADER ── */}
      <header style={{
        borderBottom: `1px solid ${T.border0}`,
        background: `linear-gradient(180deg, ${T.bg1} 0%, ${T.bg0} 100%)`,
        padding: "0 24px",
        position: "sticky", top: 0, zIndex: 100,
        backdropFilter: "blur(12px)",
      }}>
        <div style={{ maxWidth: 1280, margin: "0 auto", display: "flex", alignItems: "center", justifyContent: "space-between", height: 56 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {/* Logo mark */}
            <div style={{
              width: 32, height: 32, borderRadius: 8,
              background: `linear-gradient(135deg, ${T.cyan}20, ${T.cyan}05)`,
              border: `1px solid ${T.cyan}30`,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 14, color: T.cyan,
            }}>◆</div>
            <div>
              <h1 style={{
                fontSize: 16, fontWeight: 700, fontFamily: T.fontDisplay, color: T.t1,
                letterSpacing: "0.04em", margin: 0, lineHeight: 1,
              }}>
                IntelliAI
              </h1>
              <p style={{ fontSize: 9, color: T.t3, margin: 0, fontFamily: T.fontMono, letterSpacing: "0.08em", textTransform: "uppercase" }}>
                Intelligent MQ Topology Simplification
              </p>
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            {architectMethod && (
              <Badge
                color={architectMethod === "llm" ? T.cyan : T.amber}
                style={{ fontSize: 9 }}
              >
                {architectMethod === "llm" ? "◆ AI ARCHITECT" : "◇ RULES ENGINE"}
              </Badge>
            )}
            {result && (
              <Badge color={result.validation_passed ? T.green : T.red} style={{ fontSize: 9 }}>
                {result.validation_passed ? "✓ VALID" : "✗ VIOLATIONS"}
              </Badge>
            )}
          </div>
        </div>
      </header>

      <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
        {/* ── TAB BAR ── */}
        <nav style={{
          display: "flex", gap: 2, paddingTop: 16, paddingBottom: 0,
          borderBottom: `1px solid ${T.border0}`,
          overflowX: "auto",
        }}>
          {TAB_CONFIG.map(t => {
            const active = tab === t.id;
            const hasData = t.id === "adrs" && result?.adrs?.length;
            return (
              <button key={t.id} onClick={() => setTab(t.id)} style={{
                padding: "8px 14px", fontSize: 11, fontWeight: 500,
                border: "none", cursor: "pointer",
                background: active ? T.bg2 : "transparent",
                borderBottom: active ? `2px solid ${T.cyan}` : "2px solid transparent",
                color: active ? T.cyan : T.t3,
                fontFamily: T.fontMono, letterSpacing: "0.02em",
                display: "flex", alignItems: "center", gap: 6,
                transition: "all 0.15s", flexShrink: 0,
                borderRadius: active ? `${T.r1} ${T.r1} 0 0` : undefined,
              }}
                onMouseEnter={e => { if (!active) e.target.style.color = T.t2; }}
                onMouseLeave={e => { if (!active) e.target.style.color = T.t3; }}
              >
                <span style={{ fontSize: 11, opacity: 0.7 }}>{t.icon}</span>
                {t.label}
                {hasData ? <span style={{ fontSize: 9, padding: "1px 5px", borderRadius: 10, background: T.cyanBg, color: T.cyan }}>{result.adrs.length}</span> : null}
              </button>
            );
          })}
        </nav>

        {/* ── ERROR ── */}
        {error && (
          <div style={{
            margin: "16px 0", padding: "12px 16px", borderRadius: T.r2,
            background: T.redBg, border: `1px solid ${T.redBorder}`,
            display: "flex", alignItems: "center", gap: 10,
            animation: "fadeUp 0.3s ease-out",
          }}>
            <span style={{ fontSize: 16, color: T.red }}>⚠</span>
            <span style={{ fontSize: 12, color: T.t1 }}>{error}</span>
            <button onClick={() => setError(null)} style={{
              marginLeft: "auto", background: "none", border: "none",
              color: T.t3, cursor: "pointer", fontSize: 14,
            }}>×</button>
          </div>
        )}

        {/* ── LOADING ── */}
        {loading && tab !== "review" && <LoadingOverlay liveSession={liveSession} />}

        {/* ── CONTENT ── */}
        <div style={{ padding: "24px 0 60px" }}>

          {/* ━━━ UPLOAD TAB ━━━ */}
          {tab === "upload" && !loading && (
            <UploadTab runDemo={runDemo} handleUpload={handleUpload} />
          )}

          {/* ━━━ REVIEW TAB ━━━ */}
          {tab === "review" && result && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              {loading ? (
                <LoadingOverlay liveSession={liveSession} />
              ) : isAwaitingReview ? (
                <ReviewChatPanel
                  result={result}
                  architectMethod={architectMethod}
                  reviewLoading={reviewLoading}
                  onApprove={() => submitReview(true)}
                  onRevise={(feedback, chatHistory) => submitReview(false, false, feedback, chatHistory)}
                  onAbort={() => submitReview(false, true)}
                  sessionId={result?.session_id}
                />
              ) : (
                <EmptyState
                  icon={isApproved ? "✓" : isAborted ? "⊘" : "◇"}
                  message={
                    isApproved ? "Design approved. Outputs are available in the other tabs."
                      : isAborted ? "Pipeline aborted. Check the Trace tab for details."
                      : isRejected ? "Revision in progress — the Architect is redesigning based on your feedback."
                      : "No review pending. Run an analysis first."
                  }
                />
              )}
            </div>
          )}

          {/* ━━━ TOPOLOGY TAB ━━━ */}
          {tab === "topology" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              {/* Controls bar */}
              <div style={{ display: "flex", gap: 12, marginBottom: 14, alignItems: "center", flexWrap: "wrap" }}>
                <span style={{ fontSize: 10, color: T.t3, fontFamily: T.fontMono }}>TRACE APP:</span>
                <select
                  value={topoFilterApp}
                  onChange={e => setTopoFilterApp(e.target.value)}
                  style={{
                    padding: "5px 10px", borderRadius: T.r1, fontSize: 11, fontFamily: T.fontMono,
                    background: T.bg2, border: `1px solid ${topoFilterApp ? T.green : T.border1}`,
                    color: topoFilterApp ? T.green : T.t2, cursor: "pointer", maxWidth: 260,
                  }}
                >
                  <option value="">All apps (no filter)</option>
                  {appList.map(a => (
                    <option key={a.id} value={a.id}>{a.name} ({a.id})</option>
                  ))}
                </select>
                {topoFilterApp && (
                  <>
                    <button onClick={() => setTopoFilterApp("")} style={{
                      padding: "4px 10px", borderRadius: T.r1, fontSize: 10,
                      background: T.redBg, border: `1px solid ${T.redBorder}`,
                      color: T.red, cursor: "pointer",
                    }}>✕ Clear</button>
                    <div style={{ width: 1, height: 24, background: T.border1 }} />
                    <button onClick={() => setTopoShowQueues(!topoShowQueues)} style={{
                      padding: "6px 14px", borderRadius: T.r1, fontSize: 11, fontFamily: T.fontMono,
                      background: topoShowQueues ? T.cyanBg : T.bg2,
                      border: `1px solid ${topoShowQueues ? T.cyan : T.border1}`,
                      color: topoShowQueues ? T.cyan : T.t2, cursor: "pointer",
                    }}>
                      {topoShowQueues ? "◆ Full MQ Objects" : "◇ Show Queues"}
                    </button>
                  </>
                )}
              </div>

              {/* Main graphs — mode toggle */}
              <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
                {["split", "diff"].map(mode => (
                  <button key={mode} onClick={() => setTopoViewMode(mode)} style={{
                    padding: "6px 16px", borderRadius: T.r1, fontSize: 11, fontWeight: 600,
                    fontFamily: T.fontMono, cursor: "pointer",
                    background: topoViewMode === mode ? (mode === "diff" ? `${T.amber}18` : `${T.cyan}18`) : T.bg2,
                    border: `1px solid ${topoViewMode === mode ? (mode === "diff" ? T.amber : T.cyan) : T.border1}`,
                    color: topoViewMode === mode ? (mode === "diff" ? T.amber : T.cyan) : T.t3,
                  }}>
                    {mode === "split" ? "◫ Side-by-Side" : "◈ Diff Overlay"}
                  </button>
                ))}
              </div>

              {topoViewMode === "split" ? (
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
                  <TopologyGraph graphData={result.as_is_graph} title="As-Is Topology" height={520}
                    badge={<Badge color={T.t3} style={{ fontSize: 9 }}>CURRENT</Badge>} />
                  <TopologyGraph
                    graphData={topoFilterApp ? filteredTargetGraph : result.target_graph}
                    title={topoFilterApp ? `Target — ${topoFilterApp}` : "Target State"}
                    height={520}
                    showQueues={topoFilterApp ? topoShowQueues : false}
                    badge={topoFilterApp
                      ? <Badge color={T.green} style={{ fontSize: 9 }}>FILTERED</Badge>
                      : <Badge color={T.green} style={{ fontSize: 9 }}>OPTIMISED</Badge>
                    }
                  />
                </div>
              ) : (
                /* ── DIFF OVERLAY VIEW ── */
                <DiffTopologyView asIsGraph={result.as_is_graph} targetGraph={result.target_graph} diff={result.topology_diff} height={600} />
              )}

              {/* Summary bar */}
              <Card glow={result.validation_passed ? T.green : T.red} delay={0.1}>
                <div style={{ padding: "14px 20px", display: "flex", alignItems: "center", gap: 20, flexWrap: "wrap" }}>
                  <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
                    <span style={{ fontSize: 22, fontWeight: 700, fontFamily: T.fontDisplay, color: T.green }}>
                      {result.complexity_reduction?.reduction_pct}%
                    </span>
                    <span style={{ fontSize: 12, color: T.t3 }}>complexity reduction</span>
                  </div>
                  <div style={{ width: 1, height: 24, background: T.border1 }} />
                  <div style={{ fontSize: 12, color: T.t2, fontFamily: T.fontMono }}>
                    {result.complexity_reduction?.before} → {result.complexity_reduction?.after}
                  </div>
                  <div style={{ width: 1, height: 24, background: T.border1 }} />
                  <Badge color={result.validation_passed ? T.green : T.red}>
                    {result.validation_passed ? "✓ All constraints satisfied" : "✗ Violations found"}
                  </Badge>
                  {architectMethod && (
                    <>
                      <div style={{ width: 1, height: 24, background: T.border1 }} />
                      <Badge color={architectMethod === "llm" ? T.cyan : T.amber}>
                        {architectMethod === "llm" ? "◆ AI Architect" : "◇ Rules Engine"}
                      </Badge>
                    </>
                  )}
                </div>
              </Card>

              {/* Topology diff if available */}
              {result.topology_diff && (
                <div style={{ marginTop: 16, display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
                  {[
                    { label: "QMs Removed", value: result.topology_diff.qms_removed?.length || 0, color: T.red },
                    { label: "Channels Added", value: result.topology_diff.channels_added?.length || 0, color: T.green },
                    { label: "Channels Removed", value: result.topology_diff.channels_removed?.length || 0, color: T.red },
                    { label: "Apps Reassigned", value: result.topology_diff.apps_reassigned?.length || 0, color: T.amber },
                  ].map((d, i) => (
                    <Card key={i} delay={0.2 + i * 0.05}>
                      <div style={{ padding: "14px 16px", textAlign: "center" }}>
                        <div style={{ fontSize: 24, fontWeight: 700, fontFamily: T.fontDisplay, color: d.color }}>{d.value}</div>
                        <div style={{ fontSize: 10, color: T.t3, marginTop: 4, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em" }}>{d.label}</div>
                      </div>
                    </Card>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* ━━━ METRICS TAB ━━━ */}
          {tab === "metrics" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              <div style={{ display: "flex", justifyContent: "center", gap: 48, marginBottom: 32 }}>
                <ScoreGauge label="As-Is Score" score={result.as_is_metrics?.total_score || 0} delay={0} />
                <div style={{ display: "flex", flexDirection: "column", justifyContent: "center", alignItems: "center" }}>
                  <span style={{ fontSize: 28, color: T.green, animation: "fadeIn 0.5s ease-out 0.3s both" }}>→</span>
                  <span style={{
                    fontSize: 11, fontFamily: T.fontMono, color: T.green, fontWeight: 600,
                    animation: "countUp 0.5s ease-out 0.5s both",
                  }}>-{result.complexity_reduction?.reduction_pct}%</span>
                </div>
                <ScoreGauge label="Target Score" score={result.target_metrics?.total_score || 0} delay={0.2} />
              </div>

              <Card delay={0.3}>
                <CardHeader>Factor Breakdown</CardHeader>
                {/* Table header */}
                <div style={{
                  display: "grid", gridTemplateColumns: "1.5fr 80px 80px 100px",
                  gap: 8, padding: "8px 16px",
                  borderBottom: `1px solid ${T.border0}`,
                  fontSize: 10, fontWeight: 600, color: T.t4,
                  fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em",
                }}>
                  <span>Metric</span><span style={{ textAlign: "right" }}>Before</span>
                  <span style={{ textAlign: "right" }}>After</span><span style={{ textAlign: "right" }}>Delta</span>
                </div>
                {[
                  ["Channel Count", "channel_count", "25%"],
                  ["Coupling Index", "coupling_index", "25%"],
                  ["Routing Depth", "routing_depth", "20%"],
                  ["Fan-Out Score", "fan_out_score", "15%"],
                  ["Orphan Objects", "orphan_objects", "5%"],
                  ["Channel Sprawl", "channel_sprawl", "10%"],
                ].map(([label, key, weight], i) => (
                  <MetricRow key={key}
                    label={`${label} (${weight})`}
                    before={result.as_is_metrics?.[key] ?? "—"}
                    after={result.target_metrics?.[key] ?? "—"}
                    delay={0.05 * i} />
                ))}
              </Card>

              {result.constraint_violations?.length > 0 && (
                <div style={{ marginTop: 24 }}>
                  <SectionTitle count={result.constraint_violations.length} delay={0.4}>Constraint Violations</SectionTitle>
                  {result.constraint_violations.map((v, i) => (
                    <ViolationBadge key={i} v={v} delay={0.05 * i} />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* ━━━ ADRs TAB ━━━ */}
          {tab === "adrs" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out", maxWidth: 800 }}>
              <div style={{ marginBottom: 20, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <div>
                  <h2 style={{ fontSize: 16, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1, marginBottom: 4 }}>
                    Architecture Decision Records
                  </h2>
                  <p style={{ fontSize: 12, color: T.t3 }}>Every design decision with full context, rationale, and consequences.</p>
                </div>
                {result.adrs?.length > 0 && <Badge color={T.cyan}>{result.adrs.length} decisions</Badge>}
              </div>
              {result.adrs?.length
                ? result.adrs.map((adr, i) => <ADRCard key={i} adr={adr} delay={0.05 * i} />)
                : <EmptyState icon="◈" message="No architecture decisions recorded." />}
            </div>
          )}

          {/* ━━━ MIGRATION TAB ━━━ */}
          {tab === "migration" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              {result.migration_plan ? (
                <>
                  {/* Diff summary */}
                  {result.topology_diff && (
                    <Card delay={0.1} style={{ marginBottom: 20 }}>
                      <CardHeader right={<Badge color={T.cyan}>{result.migration_plan.total_steps} steps</Badge>}>
                        Topology Changes
                      </CardHeader>
                      <div style={{ padding: 16, display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16 }}>
                        {result.topology_diff.qms_removed?.length > 0 && (
                          <div>
                            <div style={{ fontSize: 10, fontWeight: 600, color: T.red, fontFamily: T.fontMono, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>QMs Removed</div>
                            {result.topology_diff.qms_removed.slice(0, 8).map(qm => (
                              <div key={qm} style={{ fontSize: 12, color: T.t2, padding: "3px 0", fontFamily: T.fontMono }}>{qm}</div>
                            ))}
                            {result.topology_diff.qms_removed.length > 8 && (
                              <div style={{ fontSize: 11, color: T.t4, fontStyle: "italic", paddingTop: 4 }}>+{result.topology_diff.qms_removed.length - 8} more</div>
                            )}
                          </div>
                        )}
                        {result.topology_diff.apps_reassigned?.length > 0 && (
                          <div>
                            <div style={{ fontSize: 10, fontWeight: 600, color: T.amber, fontFamily: T.fontMono, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Apps Reassigned</div>
                            {result.topology_diff.apps_reassigned.slice(0, 8).map((a, i) => (
                              <div key={i} style={{ fontSize: 11, color: T.t2, padding: "3px 0", fontFamily: T.fontMono }}>
                                {a.app_id}: <span style={{ color: T.red }}>{a.old_qm}</span> → <span style={{ color: T.green }}>{a.new_qm}</span>
                              </div>
                            ))}
                            {result.topology_diff.apps_reassigned.length > 8 && (
                              <div style={{ fontSize: 11, color: T.t4, fontStyle: "italic", paddingTop: 4 }}>+{result.topology_diff.apps_reassigned.length - 8} more</div>
                            )}
                          </div>
                        )}
                        {result.topology_diff.channels_added?.length > 0 && (
                          <div>
                            <div style={{ fontSize: 10, fontWeight: 600, color: T.green, fontFamily: T.fontMono, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Channels Added</div>
                            {result.topology_diff.channels_added.slice(0, 8).map((ch, i) => (
                              <div key={i} style={{ fontSize: 11, color: T.t2, padding: "3px 0", fontFamily: T.fontMono }}>
                                {ch[0]} → {ch[1]}
                              </div>
                            ))}
                            {result.topology_diff.channels_added.length > 8 && (
                              <div style={{ fontSize: 11, color: T.t4, fontStyle: "italic", paddingTop: 4 }}>+{result.topology_diff.channels_added.length - 8} more</div>
                            )}
                          </div>
                        )}
                      </div>
                    </Card>
                  )}

                  {/* Phase summary cards */}
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
                    {["CREATE", "REROUTE", "DRAIN", "CLEANUP"].map((phase, i) => {
                      const steps = result.migration_plan.phases?.[phase] || [];
                      const ps = PHASE_STYLES[phase];
                      return (
                        <Card key={phase} delay={0.1 + i * 0.05} glow={steps.length > 0 ? ps.color : undefined}>
                          <div style={{ padding: "14px 16px", textAlign: "center" }}>
                            <div style={{ fontSize: 22, color: ps.color, marginBottom: 4 }}>{ps.icon}</div>
                            <div style={{ fontSize: 10, fontWeight: 600, fontFamily: T.fontMono, color: ps.color, textTransform: "uppercase", letterSpacing: "0.06em" }}>{phase}</div>
                            <div style={{ fontSize: 20, fontWeight: 700, fontFamily: T.fontDisplay, color: T.t1, marginTop: 6 }}>{steps.length}</div>
                            <div style={{ fontSize: 10, color: T.t4 }}>steps</div>
                          </div>
                        </Card>
                      );
                    })}
                  </div>

                  {/* Download button */}
                  <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 16 }}>
                    <DownloadButton label="Download Full Plan" onClick={() => {
                      const text = result.migration_plan.steps?.map(s =>
                        `Step ${s.step_number} [${s.phase}] — ${s.description}\nTarget: ${s.target_qm}\nForward MQSC:\n${s.mqsc_forward || "N/A"}\nRollback MQSC:\n${s.mqsc_rollback || "N/A"}\nVerification:\n${s.verification || "N/A"}\n${"─".repeat(60)}`
                      ).join("\n\n") || "No steps";
                      downloadFile(text, "migration_plan.txt");
                    }} />
                  </div>

                  {/* Steps grouped by phase in collapsible panels */}
                  {["CREATE", "REROUTE", "DRAIN", "CLEANUP"].map((phase, pi) => {
                    const steps = result.migration_plan.phases?.[phase] || [];
                    if (steps.length === 0) return null;
                    const ps = PHASE_STYLES[phase];
                    const phaseDesc = {
                      CREATE: "Provision new queue managers, channels, and routing infrastructure",
                      REROUTE: "Migrate applications to their new dedicated queue managers",
                      DRAIN: "Wait for in-flight messages to complete on old transmission queues",
                      CLEANUP: "Decommission old channels, queues, and orphan queue managers",
                    };
                    return (
                      <CollapsibleSection
                        key={phase}
                        title={`Phase ${pi + 1}: ${phase}`}
                        icon={ps.icon}
                        color={ps.color}
                        count={steps.length}
                        subtitle={phaseDesc[phase]}
                        defaultOpen={pi === 0}
                        delay={0.15 * pi}
                      >
                        {steps.map((step, i) => (
                          <MigrationStep key={i} step={step} delay={0.02 * i} />
                        ))}
                      </CollapsibleSection>
                    );
                  })}
                </>
              ) : (
                <EmptyState icon="⇄" message={
                  result.awaiting_human_review
                    ? "Approve the design in the Review tab first. The migration plan is generated after approval."
                    : "No migration plan data was returned. Check the Trace tab for pipeline details."
                } />
              )}
            </div>
          )}

          {/* ━━━ MQSC TAB ━━━ */}
          {tab === "mqsc" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              {result.mqsc_scripts?.length > 0 ? (
                <>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
                    <div>
                      <h2 style={{ fontSize: 16, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1, marginBottom: 4 }}>
                        MQSC Provisioning Scripts
                      </h2>
                      <p style={{ fontSize: 12, color: T.t3 }}>Per-QM runmqsc commands for the target state. Ready to execute.</p>
                    </div>
                    <DownloadButton label="Download All" onClick={() => {
                      downloadFile(result.mqsc_scripts?.join("\n") || "", "intelli_ai_target.mqsc");
                    }} />
                  </div>
                  <Card>
                    <pre style={{
                      padding: 20, fontSize: 11, lineHeight: 1.8,
                      fontFamily: T.fontMono, color: T.t2,
                      overflowX: "auto", maxHeight: 600,
                      margin: 0,
                    }}>
                      {result.mqsc_scripts.join("\n")}
                    </pre>
                  </Card>
                </>
              ) : (
                <EmptyState icon="▸" message={
                  result.awaiting_human_review
                    ? "Approve the design in the Review tab first. MQSC scripts are generated after approval."
                    : "No MQSC scripts were returned. Check the Trace tab for pipeline details."
                } />
              )}
            </div>
          )}

          {/* ━━━ CSVS TAB ━━━ */}
          {tab === "csvs" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              <div style={{ marginBottom: 20 }}>
                <h2 style={{ fontSize: 16, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1, marginBottom: 4 }}>
                  Deliverables & Downloads
                </h2>
                <p style={{ fontSize: 12, color: T.t3 }}>All generated files grouped by category. Click a section to expand.</p>
              </div>

              {result.target_csvs && Object.keys(result.target_csvs).length > 0
                ? (() => {
                    const csvs = result.target_csvs;

                    // ── KEY DELIVERABLES — highlighted hero cards ──────────────
                    const keyFiles = [
                      {
                        key: "MQ_Raw_Data_Target",
                        title: "MQ_Raw_Data_Target.csv",
                        subtitle: "Same 29 columns as input — re-upload to verify score drops",
                        desc: "The primary deliverable. Identical structure to the uploaded CSV but reflecting the optimised 1:1 target state. Feed it back into IntelliAI or any provisioning tool.",
                        icon: "⊞",
                        color: T.cyan,
                        ext: ".csv",
                        mime: "text/csv",
                        badge: "PRIMARY OUTPUT",
                      },
                      {
                        key: "target-topology",
                        title: "target-topology.json",
                        subtitle: "Structured JSON — machine-readable target state",
                        desc: "Full target topology with queue managers, channels, applications, and queue objects. Schema designed for automation-first provisioning.",
                        icon: "◇",
                        color: T.green,
                        ext: ".json",
                        mime: "application/json",
                        badge: "JSON TOPOLOGY",
                      },
                      {
                        key: "insights",
                        title: "insights.md",
                        subtitle: "Strategic recommendation — MQ vs Kafka migration framework",
                        desc: "Non-obvious findings from both source and target topologies. Includes data-driven Kafka migration signals based on actual fan-out, coupling, and channel metrics.",
                        icon: "◈",
                        color: T.amber,
                        ext: ".md",
                        mime: "text/markdown",
                        badge: "STRATEGIC INSIGHTS",
                      },
                    ];

                    // ── Group remaining files ─────────────────────────────────
                    const heroKeys = new Set(keyFiles.map(f => f.key));
                    const entries = Object.entries(csvs).filter(([name]) => !heroKeys.has(name));
                    const groups = {
                      "Other Target Data": { icon: "◇", color: T.cyan, files: [] },
                      "Analysis & Documentation": { icon: "◈", color: T.green, files: [] },
                      "MQSC Scripts": { icon: "▸", color: T.amber, files: [] },
                    };
                    entries.forEach(([name, content]) => {
                      if (name.startsWith("mqsc_")) {
                        groups["MQSC Scripts"].files.push([name, content]);
                      } else if (["target_queue_managers", "target_channels", "target_queues", "target_applications"].includes(name)) {
                        groups["Other Target Data"].files.push([name, content]);
                      } else {
                        groups["Analysis & Documentation"].files.push([name, content]);
                      }
                    });

                    return (
                      <>
                        {/* ── HERO CARDS for 3 key deliverables ── */}
                        <SectionTitle delay={0}>Key Deliverables</SectionTitle>
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 14, marginBottom: 28 }}>
                          {keyFiles.map((f, i) => {
                            const content = csvs[f.key];
                            if (!content) return (
                              <Card key={f.key} delay={0.1 * i} style={{ opacity: 0.5 }}>
                                <div style={{ padding: "20px 16px", textAlign: "center" }}>
                                  <div style={{ fontSize: 24, color: T.t4, marginBottom: 8 }}>{f.icon}</div>
                                  <div style={{ fontSize: 12, fontWeight: 600, color: T.t4 }}>{f.title}</div>
                                  <div style={{ fontSize: 10, color: T.t4, marginTop: 4 }}>Not yet generated — approve design first</div>
                                </div>
                              </Card>
                            );
                            const lines = content.trim().split("\n");
                            const isJson = content.trim().startsWith("{") || content.trim().startsWith("[");
                            const rowCount = isJson ? null : lines.length - 1;
                            return (
                              <Card key={f.key} glow={f.color} delay={0.1 * i}>
                                <div style={{
                                  padding: "4px 16px 4px",
                                  background: `${f.color}08`,
                                  borderBottom: `1px solid ${f.color}20`,
                                  display: "flex", alignItems: "center", justifyContent: "space-between",
                                }}>
                                  <Badge color={f.color} style={{ fontSize: 8, fontWeight: 700 }}>{f.badge}</Badge>
                                  <span style={{ fontSize: 9, color: T.t4, fontFamily: T.fontMono }}>{f.ext}</span>
                                </div>
                                <div style={{ padding: "16px" }}>
                                  <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                                    <span style={{ fontSize: 18, color: f.color }}>{f.icon}</span>
                                    <div>
                                      <div style={{ fontSize: 13, fontWeight: 600, fontFamily: T.fontMono, color: T.t1 }}>{f.title}</div>
                                      <div style={{ fontSize: 10, color: T.t3, marginTop: 2 }}>{f.subtitle}</div>
                                    </div>
                                  </div>
                                  <p style={{ fontSize: 11, color: T.t3, lineHeight: 1.6, margin: "10px 0 14px" }}>{f.desc}</p>
                                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                                    <div style={{ display: "flex", gap: 6 }}>
                                      {rowCount != null && <Badge color={T.t3} style={{ fontSize: 9 }}>{rowCount} rows</Badge>}
                                      <Badge color={T.t3} style={{ fontSize: 9 }}>{(content.length / 1024).toFixed(1)} KB</Badge>
                                    </div>
                                    <DownloadButton label="Download" onClick={() => downloadFile(content, f.title, f.mime)} />
                                  </div>
                                </div>
                                {/* Preview */}
                                <div style={{ borderTop: `1px solid ${T.border0}` }}>
                                  <pre style={{
                                    padding: "10px 16px", margin: 0,
                                    fontSize: 9, fontFamily: T.fontMono,
                                    color: T.t4, lineHeight: 1.5,
                                    overflowX: "auto", maxHeight: 80,
                                  }}>
{lines.slice(0, 4).join("\n")}
{lines.length > 4 ? "\n…" : ""}
                                  </pre>
                                </div>
                              </Card>
                            );
                          })}
                        </div>

                        {/* ── GROUPED SECTIONS for remaining files ── */}
                        <SectionTitle delay={0.3}>All Files</SectionTitle>
                        {Object.entries(groups).map(([groupName, group], gi) => {
                          if (group.files.length === 0) return null;
                          return (
                            <CollapsibleSection
                              key={groupName}
                              title={groupName}
                              icon={group.icon}
                              color={group.color}
                              count={group.files.length}
                              defaultOpen={gi === 0}
                              delay={0.35 + 0.1 * gi}
                            >
                              {group.files.map(([name, content], idx) => {
                                const isJson = content.trim().startsWith("{") || content.trim().startsWith("[");
                                const ext = isJson ? ".json" : name.startsWith("mqsc_") ? ".mqsc" : ".csv";
                                const mime = isJson ? "application/json" : "text/csv";
                                const lines = content.trim().split("\n");
                                const rowCount = isJson ? null : lines.length - 1;
                                const displayName = name.startsWith("mqsc_") ? name.replace("mqsc_", "") + " (MQSC)" : name;
                                return (
                                  <div key={name} style={{
                                    padding: "12px 16px",
                                    borderBottom: idx < group.files.length - 1 ? `1px solid ${T.border0}` : "none",
                                    display: "flex", alignItems: "center", justifyContent: "space-between",
                                    gap: 12,
                                  }}>
                                    <div style={{ flex: 1, minWidth: 0 }}>
                                      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                                        <span style={{ fontSize: 12, fontFamily: T.fontMono, color: T.t1, fontWeight: 500 }}>
                                          {displayName}{ext}
                                        </span>
                                        {rowCount != null && <Badge color={T.t3} style={{ fontSize: 9 }}>{rowCount} rows</Badge>}
                                        {isJson && <Badge color={T.cyan} style={{ fontSize: 9 }}>JSON</Badge>}
                                        {name.startsWith("mqsc_") && <Badge color={T.amber} style={{ fontSize: 9 }}>MQSC</Badge>}
                                      </div>
                                      <div style={{ fontSize: 10, color: T.t4, fontFamily: T.fontMono, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                        {lines[0]?.slice(0, 80)}{lines[0]?.length > 80 ? "…" : ""}
                                      </div>
                                    </div>
                                    <DownloadButton label="Download" onClick={() => downloadFile(content, displayName.replace(" (MQSC)", "") + ext, mime)} />
                                  </div>
                                );
                              })}
                            </CollapsibleSection>
                          );
                        })}
                      </>
                    );
                  })()
                : <EmptyState icon="⊞" message={
                    result.awaiting_human_review
                      ? "Approve the design in the Review tab first. Target CSVs are generated after approval."
                      : "No CSV output was returned. Check the Trace tab for pipeline details."
                  } />}
            </div>
          )}

          {/* ━━━ REPORT TAB ━━━ */}
          {tab === "report" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              {result.final_report ? (
                <ReportViewer report={result.final_report} downloadFile={downloadFile} />
              ) : (
                <EmptyState icon="◫" message={
                  result.awaiting_human_review
                    ? "Approve the design in the Review tab first. The report is generated after approval."
                    : "No report was returned. Check the Trace tab for pipeline details."
                } />
              )}
            </div>
          )}

          {/* ━━━ TRACE TAB ━━━ */}
          {/* ── SOLVER TAB — depth-visibility, the "no blackboxes" pitch ── */}
          {tab === "solver" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out", display: "flex", flexDirection: "column", gap: 16 }}>
              <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
                <div>
                  <h2 style={{ fontSize: 16, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1, marginBottom: 4 }}>
                    Solver Performance
                  </h2>
                  <p style={{ fontSize: 12, color: T.t3 }}>
                    Optimization algorithm output, math under the hood, and provable guarantees. Every number on this page traces back to a paper you can read.
                  </p>
                </div>
                {result.session_id && (
                  <a
                    href={`${API}/api/session/${result.session_id}/evidence`}
                    style={{
                      display: "inline-flex", alignItems: "center", gap: 8,
                      padding: "10px 16px", borderRadius: T.r1,
                      background: T.cyanBg, border: `1px solid ${T.cyanBorder}`,
                      color: T.cyan, fontSize: 12, fontWeight: 600, fontFamily: T.fontMono,
                      cursor: "pointer", textDecoration: "none",
                      textTransform: "uppercase", letterSpacing: "0.06em",
                      flexShrink: 0,
                      transition: "all 0.15s",
                    }}
                    onMouseEnter={e => {
                      e.currentTarget.style.background = T.cyan;
                      e.currentTarget.style.color = T.bg0;
                    }}
                    onMouseLeave={e => {
                      e.currentTarget.style.background = T.cyanBg;
                      e.currentTarget.style.color = T.cyan;
                    }}
                  >
                    <span style={{ fontSize: 14 }}>↓</span>
                    Evidence Bundle (.zip)
                  </a>
                )}
              </div>

              {!result.solver_run ? (
                <EmptyState icon="Σ" message="No solver telemetry — pipeline used legacy MST path or solver flag was off." />
              ) : (
                <>
                  {/* HEADLINE CHANNEL CASCADE */}
                  <Card delay={0.05}>
                    <CardHeader right={
                      <Badge color={result.solver_run.integrity_check_passed ? T.green : T.red}>
                        {result.solver_run.integrity_check_passed ? "✓ Integrity Verified" : "✗ Integrity Failure"}
                      </Badge>
                    }>Channel Reduction Cascade</CardHeader>
                    <div style={{ padding: "20px 24px" }}>
                      <div style={{
                        display: "grid",
                        gridTemplateColumns: "1fr auto 1fr auto 1fr",
                        gap: 16, alignItems: "center",
                      }}>
                        {/* As-Is */}
                        <div style={{ textAlign: "center" }}>
                          <div style={{ fontSize: 36, fontWeight: 700, fontFamily: T.fontDisplay, color: T.red }}>
                            {result.solver_run.asis_channel_count ?? "—"}
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 4, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.08em" }}>
                            As-Is
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 2 }}>(input topology)</div>
                        </div>
                        <div style={{ fontSize: 20, color: T.t3 }}>→</div>
                        {/* Architect */}
                        <div style={{ textAlign: "center" }}>
                          <div style={{ fontSize: 36, fontWeight: 700, fontFamily: T.fontDisplay, color: T.amber }}>
                            {result.solver_run.architect_channel_count ?? result.solver_run.initial_channel_count}
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 4, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.08em" }}>
                            Architect
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 2 }}>(after AI re-design + backfill)</div>
                        </div>
                        <div style={{ fontSize: 20, color: T.t3 }}>→</div>
                        {/* Solver */}
                        <div style={{ textAlign: "center" }}>
                          <div style={{ fontSize: 36, fontWeight: 700, fontFamily: T.fontDisplay, color: T.green }}>
                            {result.solver_run.actual_channel_count ?? result.solver_run.final_channel_count}
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 4, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.08em" }}>
                            Solver
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 2 }}>(directed Steiner network)</div>
                        </div>
                      </div>

                      {/* Reduction summary line */}
                      <div style={{ marginTop: 24, paddingTop: 20, borderTop: `1px solid ${T.border0}`, display: "flex", justifyContent: "space-around" }}>
                        {result.solver_run.delta_pct_vs_asis != null && (
                          <div style={{ textAlign: "center" }}>
                            <div style={{ fontSize: 20, fontWeight: 600, fontFamily: T.fontDisplay, color: T.cyan }}>
                              {(-result.solver_run.delta_pct_vs_asis).toFixed(1)}%
                            </div>
                            <div style={{ fontSize: 10, color: T.t3, marginTop: 4, fontFamily: T.fontMono, textTransform: "uppercase" }}>
                              reduction vs as-is
                            </div>
                          </div>
                        )}
                        {result.solver_run.delta_pct_vs_architect != null && (
                          <div style={{ textAlign: "center" }}>
                            <div style={{ fontSize: 20, fontWeight: 600, fontFamily: T.fontDisplay, color: T.cyan }}>
                              {(-result.solver_run.delta_pct_vs_architect).toFixed(1)}%
                            </div>
                            <div style={{ fontSize: 10, color: T.t3, marginTop: 4, fontFamily: T.fontMono, textTransform: "uppercase" }}>
                              reduction vs architect
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </Card>

                  {/* TWO-COLUMN: Algorithm + Math */}
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                    {/* Algorithm card */}
                    <Card delay={0.1}>
                      <CardHeader right={<Badge color={T.purple}>{result.solver_run.method ?? "unknown"}</Badge>}>
                        Algorithm
                      </CardHeader>
                      <div style={{ padding: "16px 20px", display: "flex", flexDirection: "column", gap: 12 }}>
                        <div>
                          <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 4 }}>
                            Approach
                          </div>
                          <div style={{ fontSize: 13, color: T.t1, lineHeight: 1.5 }}>
                            {result.solver_run.method === "steiner_local_search" ?
                              "Greedy channel-removal local search on the directed Steiner network formulation. Each producer→consumer pair is a connectivity requirement; the algorithm removes channels iteratively whenever the per-edge saving (α saved minus β·extra-hops introduced) is positive across all affected pairs."
                              : "Multi-commodity flow with fixed channel charges, solved via CP-SAT exact integer programming. Used for small benchmarks where exact optima are tractable."
                            }
                          </div>
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 4 }}>
                            Citation
                          </div>
                          <div style={{ fontSize: 12, color: T.t2, lineHeight: 1.5, fontStyle: "italic" }}>
                            {result.solver_run.method === "steiner_local_search" ?
                              "Charikar, Chekuri, Cheung, Dai, Goel, Guha, Li (1999). \"Approximation algorithms for directed Steiner problems.\" Journal of Algorithms 33(1):73–91."
                              : "Magnanti, T. L., & Wong, R. T. (1984). \"Network design and transportation planning: Models and algorithms.\" Transportation Science 18(1):1–55."
                            }
                          </div>
                        </div>
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                          <Stat label="Iterations" value={result.solver_run.iterations ?? "?"} delay={0.15} />
                          <Stat label="Solve Time" value={`${(result.solver_run.solve_time_s ?? 0).toFixed(2)}s`} color={T.cyan} delay={0.2} />
                        </div>
                      </div>
                    </Card>

                    {/* Math card */}
                    <Card delay={0.15}>
                      <CardHeader>Objective Function</CardHeader>
                      <div style={{ padding: "16px 20px", display: "flex", flexDirection: "column", gap: 12 }}>
                        <div style={{
                          padding: 14, borderRadius: T.r1,
                          background: T.bg3, border: `1px solid ${T.border0}`,
                          fontFamily: T.fontMono, fontSize: 13, color: T.t1, textAlign: "center",
                        }}>
                          minimize {" "}
                          <span style={{ color: T.cyan }}>α·|C|</span>
                          {" + "}
                          <span style={{ color: T.green }}>β·Σhops</span>
                          {" + "}
                          <span style={{ color: T.amber }}>γ·penalties</span>
                        </div>
                        <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "6px 16px", fontSize: 11, fontFamily: T.fontMono }}>
                          <span style={{ color: T.cyan }}>α (channel cost)</span>
                          <span style={{ color: T.t2 }}>{result.solver_run.alpha ?? "?"}</span>
                          <span style={{ color: T.green }}>β (hop cost)</span>
                          <span style={{ color: T.t2 }}>{result.solver_run.beta ?? "?"}</span>
                          <span style={{ color: T.amber }}>γ (penalty weight)</span>
                          <span style={{ color: T.t2 }}>{result.solver_run.gamma ?? "?"}</span>
                        </div>
                        {result.solver_run.objective_breakdown && (
                          <div style={{ paddingTop: 8, borderTop: `1px solid ${T.border0}` }}>
                            <div style={{ fontSize: 10, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
                              Final Objective Breakdown
                            </div>
                            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr auto", gap: "4px 12px", fontSize: 12, alignItems: "center" }}>
                              <span style={{ color: T.cyan }}>channels</span>
                              <ProgressBar value={result.solver_run.objective_breakdown.channels} max={result.solver_run.objective_value} color={T.cyan} />
                              <span style={{ color: T.t2, fontFamily: T.fontMono }}>{(result.solver_run.objective_breakdown.channels ?? 0).toFixed(1)}</span>
                              <span style={{ color: T.green }}>hops</span>
                              <ProgressBar value={result.solver_run.objective_breakdown.hops} max={result.solver_run.objective_value} color={T.green} />
                              <span style={{ color: T.t2, fontFamily: T.fontMono }}>{(result.solver_run.objective_breakdown.hops ?? 0).toFixed(1)}</span>
                              <span style={{ color: T.amber }}>penalties</span>
                              <ProgressBar value={result.solver_run.objective_breakdown.penalties} max={result.solver_run.objective_value} color={T.amber} />
                              <span style={{ color: T.t2, fontFamily: T.fontMono }}>{(result.solver_run.objective_breakdown.penalties ?? 0).toFixed(1)}</span>
                            </div>
                            <div style={{ marginTop: 8, paddingTop: 8, borderTop: `1px solid ${T.border0}`, fontSize: 12, color: T.t1, display: "flex", justifyContent: "space-between" }}>
                              <span style={{ fontFamily: T.fontMono }}>Total Objective</span>
                              <span style={{ fontFamily: T.fontMono, fontWeight: 600 }}>{(result.solver_run.objective_value ?? 0).toFixed(1)}</span>
                            </div>
                          </div>
                        )}
                      </div>
                    </Card>
                  </div>

                  {/* OPTIMALITY GUARANTEES */}
                  <Card delay={0.2}>
                    <CardHeader right={<Badge color={T.green}>2-approximation</Badge>}>Optimality Guarantees</CardHeader>
                    <div style={{ padding: "16px 20px" }}>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                        {/* Algorithmic guarantee — the meaningful one */}
                        <div style={{
                          padding: 16, borderRadius: T.r1,
                          background: T.greenBg, border: `1px solid ${T.greenBorder}`,
                        }}>
                          <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>
                            Algorithmic max gap
                          </div>
                          <div style={{ fontSize: 28, fontWeight: 700, fontFamily: T.fontDisplay, color: T.green, marginBottom: 4 }}>
                            ≤ {(result.solver_run.max_optimality_gap_pct ?? 100).toFixed(0)}%
                          </div>
                          <div style={{ fontSize: 11, color: T.t2, lineHeight: 1.5 }}>
                            Provable upper bound on the gap between our solution and the true optimum, from the algorithm's worst-case approximation ratio. <span style={{ color: T.green }}>This is the meaningful number.</span>
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 8, fontStyle: "italic" }}>
                            via Charikar et al. 1999, J.Algorithms 33:73-91
                          </div>
                        </div>
                        {/* LP-relaxation gap — the loose one */}
                        <div style={{
                          padding: 16, borderRadius: T.r1,
                          background: T.bg3, border: `1px solid ${T.border0}`,
                        }}>
                          <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>
                            LP-relaxation gap
                          </div>
                          <div style={{ fontSize: 28, fontWeight: 700, fontFamily: T.fontDisplay, color: T.amber, marginBottom: 4 }}>
                            {(result.solver_run.lp_gap_pct_uncapped ?? result.solver_run.gap_pct ?? 0).toFixed(0)}%
                          </div>
                          <div style={{ fontSize: 11, color: T.t2, lineHeight: 1.5 }}>
                            Gap to our LP-relaxation lower bound. <span style={{ color: T.amber }}>Loose on uniform-cost complete graphs</span> by design — see Wong 1984 for why; tighter bounds become available when business constraints structure the candidate edge set.
                          </div>
                          <div style={{ fontSize: 10, color: T.t3, marginTop: 8, fontStyle: "italic" }}>
                            objective={(result.solver_run.objective_value ?? 0).toFixed(1)} / lower_bound={(result.solver_run.lower_bound ?? 0).toFixed(1)}
                          </div>
                        </div>
                      </div>
                    </div>
                  </Card>

                  {/* WHAT WE DID NOT DO — institutional trust signal */}
                  <Card delay={0.25}>
                    <CardHeader>What This Solver Does NOT Use</CardHeader>
                    <div style={{ padding: "16px 20px" }}>
                      <p style={{ fontSize: 12, color: T.t2, marginBottom: 12, lineHeight: 1.6 }}>
                        Naming what we restrained ourselves from is part of the engineering discipline. Each of these alternatives was considered and rejected with a defensible reason.
                      </p>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                        {[
                          { name: "Reinforcement learning", reason: "no training data, no time horizon for episodic learning" },
                          { name: "Graph Neural Networks", reason: "no learning problem with our instance scale" },
                          { name: "MILP / CP-SAT at scale", reason: "~10⁹ binary vars at production scale, doesn't load" },
                          { name: "Quantum annealing", reason: "not applicable to combinatorial network design" },
                          { name: "LLM-driven channel selection", reason: "non-deterministic, unauditable; LLM hints are advisory only" },
                          { name: "Fully autonomous mode", reason: "human approval gate preserved on every state change" },
                        ].map((x, i) => (
                          <div key={i} style={{
                            padding: "8px 12px", borderRadius: T.r1,
                            background: T.bg3, border: `1px solid ${T.border0}`,
                          }}>
                            <div style={{ fontSize: 11, color: T.t1, fontWeight: 600, marginBottom: 2 }}>
                              <span style={{ color: T.red, marginRight: 6 }}>✗</span>{x.name}
                            </div>
                            <div style={{ fontSize: 10, color: T.t3, lineHeight: 1.4, paddingLeft: 14 }}>{x.reason}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </Card>
                </>
              )}
            </div>
          )}

          {/* ── COMPLIANCE TAB — LLM auditor findings + V-009 reachability ── */}
          {tab === "compliance" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out", display: "flex", flexDirection: "column", gap: 16 }}>
              <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
                <div>
                  <h2 style={{ fontSize: 16, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1, marginBottom: 4 }}>
                    Compliance &amp; Validation
                  </h2>
                  <p style={{ fontSize: 12, color: T.t3 }}>
                    LLM-driven compliance audit, formal-style invariant validation, and constraint enforcement results.
                  </p>
                </div>
                {result.session_id && (
                  <a
                    href={`${API}/api/session/${result.session_id}/evidence`}
                    style={{
                      display: "inline-flex", alignItems: "center", gap: 8,
                      padding: "10px 16px", borderRadius: T.r1,
                      background: T.cyanBg, border: `1px solid ${T.cyanBorder}`,
                      color: T.cyan, fontSize: 12, fontWeight: 600, fontFamily: T.fontMono,
                      cursor: "pointer", textDecoration: "none",
                      textTransform: "uppercase", letterSpacing: "0.06em",
                      flexShrink: 0,
                      transition: "all 0.15s",
                    }}
                    onMouseEnter={e => {
                      e.currentTarget.style.background = T.cyan;
                      e.currentTarget.style.color = T.bg0;
                    }}
                    onMouseLeave={e => {
                      e.currentTarget.style.background = T.cyanBg;
                      e.currentTarget.style.color = T.cyan;
                    }}
                  >
                    <span style={{ fontSize: 14 }}>↓</span>
                    Evidence Bundle (.zip)
                  </a>
                )}
              </div>

              {/* TOP ROW: Compliance Score + V-009 Reachability + Pipeline Status */}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 14 }}>
                {/* Compliance Score */}
                <Card delay={0.05} glow={
                  (result.compliance_audit?.compliance_score ?? 0) >= 80 ? T.green
                  : (result.compliance_audit?.compliance_score ?? 0) >= 60 ? T.amber : T.red
                }>
                  <div style={{ padding: "20px 24px", textAlign: "center" }}>
                    <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>
                      Compliance Score
                    </div>
                    <div style={{
                      fontSize: 48, fontWeight: 700, fontFamily: T.fontDisplay,
                      color: (result.compliance_audit?.compliance_score ?? 0) >= 80 ? T.green
                            : (result.compliance_audit?.compliance_score ?? 0) >= 60 ? T.amber : T.red,
                    }}>
                      {result.compliance_audit?.compliance_score ?? "—"}
                      <span style={{ fontSize: 20, color: T.t3 }}>/100</span>
                    </div>
                    <div style={{ fontSize: 11, color: T.t3, marginTop: 8 }}>
                      LLM auditor (llama-3.3-70b-versatile)
                    </div>
                  </div>
                </Card>

                {/* V-009 Reachability */}
                <Card delay={0.1} glow={
                  (result.constraint_violations?.filter(v => v.rule === "REQUIRED_PAIR_REACHABILITY")?.length ?? 0) === 0
                    ? T.green : T.red
                }>
                  <div style={{ padding: "20px 24px", textAlign: "center" }}>
                    <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>
                      V-009 Reachability
                    </div>
                    {(() => {
                      const sr = result.solver_run;
                      const violations = result.constraint_violations?.filter(v => v.rule === "REQUIRED_PAIR_REACHABILITY") ?? [];
                      const pairCount = sr?.n_pairs ?? "?";
                      const reachable = pairCount !== "?" ? pairCount - violations.length : "?";
                      return (
                        <>
                          <div style={{
                            fontSize: 48, fontWeight: 700, fontFamily: T.fontDisplay,
                            color: violations.length === 0 ? T.green : T.red,
                          }}>
                            {pairCount !== "?" && violations.length === 0 ? "100%" : violations.length === 0 ? "✓" : "✗"}
                          </div>
                          <div style={{ fontSize: 11, color: T.t3, marginTop: 8, fontFamily: T.fontMono }}>
                            {reachable !== "?" && pairCount !== "?" ? `${reachable} / ${pairCount} pairs` : "every producer→consumer path verified"}
                          </div>
                        </>
                      );
                    })()}
                  </div>
                </Card>

                {/* Validation Pass */}
                <Card delay={0.15} glow={result.validation_passed ? T.green : T.red}>
                  <div style={{ padding: "20px 24px", textAlign: "center" }}>
                    <div style={{ fontSize: 11, color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>
                      Constraint Validation
                    </div>
                    <div style={{
                      fontSize: 48, fontWeight: 700, fontFamily: T.fontDisplay,
                      color: result.validation_passed ? T.green : T.red,
                    }}>
                      {result.validation_passed ? "PASS" : "FAIL"}
                    </div>
                    <div style={{ fontSize: 11, color: T.t3, marginTop: 8 }}>
                      {(result.constraint_violations?.length ?? 0)} total violations
                      {result.constraint_violations?.length > 0 && (
                        <> ({result.constraint_violations.filter(v => v.severity === "CRITICAL").length} critical)</>
                      )}
                    </div>
                  </div>
                </Card>
              </div>

              {/* HA + Security Assessment */}
              {(result.compliance_audit?.ha_assessment || result.compliance_audit?.security_assessment) && (
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                  {result.compliance_audit?.ha_assessment && (
                    <Card delay={0.2}>
                      <CardHeader right={
                        <Badge color={result.compliance_audit.ha_assessment.has_redundancy ? T.green : T.amber}>
                          {result.compliance_audit.ha_assessment.has_redundancy ? "Redundant" : "No Redundancy"}
                        </Badge>
                      }>High Availability</CardHeader>
                      <div style={{ padding: "16px 20px", display: "flex", flexDirection: "column", gap: 10 }}>
                        <div style={{ display: "flex", justifyContent: "space-between" }}>
                          <span style={{ fontSize: 12, color: T.t3 }}>Single points of failure</span>
                          <span style={{
                            fontSize: 14, fontWeight: 600, fontFamily: T.fontMono,
                            color: (result.compliance_audit.ha_assessment.spof_count ?? 0) === 0 ? T.green : T.amber,
                          }}>
                            {result.compliance_audit.ha_assessment.spof_count ?? "?"}
                          </span>
                        </div>
                        {result.compliance_audit.ha_assessment.recommendation && (
                          <div style={{ fontSize: 11, color: T.t2, lineHeight: 1.5, paddingTop: 10, borderTop: `1px solid ${T.border0}` }}>
                            <span style={{ color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", fontSize: 9, letterSpacing: "0.06em", display: "block", marginBottom: 4 }}>Recommendation</span>
                            {result.compliance_audit.ha_assessment.recommendation}
                          </div>
                        )}
                      </div>
                    </Card>
                  )}
                  {result.compliance_audit?.security_assessment && (
                    <Card delay={0.25}>
                      <CardHeader right={
                        <Badge color={
                          (result.compliance_audit.security_assessment.channel_security_score ?? 0) >= 80 ? T.green
                          : (result.compliance_audit.security_assessment.channel_security_score ?? 0) >= 60 ? T.amber
                          : T.red
                        }>
                          {result.compliance_audit.security_assessment.channel_security_score ?? "?"}/100
                        </Badge>
                      }>Channel Security</CardHeader>
                      <div style={{ padding: "16px 20px", display: "flex", flexDirection: "column", gap: 10 }}>
                        <div style={{ display: "flex", justifyContent: "space-between" }}>
                          <span style={{ fontSize: 12, color: T.t3 }}>SSL/TLS recommended</span>
                          <Badge color={result.compliance_audit.security_assessment.ssl_tls_recommended ? T.amber : T.green} style={{ fontSize: 10 }}>
                            {result.compliance_audit.security_assessment.ssl_tls_recommended ? "YES — currently absent" : "✓ Configured"}
                          </Badge>
                        </div>
                        {(result.compliance_audit.security_assessment.auth_gaps ?? []).length > 0 && (
                          <div style={{ fontSize: 11, color: T.t2, paddingTop: 10, borderTop: `1px solid ${T.border0}` }}>
                            <span style={{ color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", fontSize: 9, letterSpacing: "0.06em", display: "block", marginBottom: 4 }}>Auth Gaps</span>
                            <ul style={{ paddingLeft: 16, margin: 0 }}>
                              {result.compliance_audit.security_assessment.auth_gaps.slice(0, 5).map((g, i) => (
                                <li key={i} style={{ marginBottom: 2 }}>{g}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    </Card>
                  )}
                </div>
              )}

              {/* FINDINGS LIST */}
              {result.compliance_audit?.findings?.length > 0 && (
                <div>
                  <SectionTitle count={result.compliance_audit.findings.length} delay={0.3}>LLM Compliance Findings</SectionTitle>
                  <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                    {result.compliance_audit.findings.map((f, i) => {
                      const sev = f.severity || "INFO";
                      const sevColor = sev === "CRITICAL" ? T.red
                                      : sev === "HIGH" ? T.red
                                      : sev === "MEDIUM" ? T.amber
                                      : sev === "LOW" ? T.cyan
                                      : T.t3;
                      const findingText = f.finding || f.description || f.detail || f.issue || "(no description provided by LLM)";
                      return (
                        <Card key={i} delay={0.35 + 0.04 * i} glow={sev === "CRITICAL" || sev === "HIGH" ? sevColor : null}>
                          <div style={{ padding: "12px 16px" }}>
                            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                              <Badge color={sevColor} style={{ fontSize: 9 }}>{sev}</Badge>
                              <Badge color={T.t3} style={{ fontSize: 9 }}>{f.category || "GENERAL"}</Badge>
                              {f.affected_entities?.length > 0 && (
                                <span style={{ fontSize: 10, color: T.t3, fontFamily: T.fontMono, marginLeft: "auto" }}>
                                  affects {f.affected_entities.length} {f.affected_entities.length === 1 ? "entity" : "entities"}
                                </span>
                              )}
                            </div>
                            <div style={{ fontSize: 13, color: T.t1, marginBottom: 8, lineHeight: 1.5 }}>
                              {findingText}
                            </div>
                            {f.recommendation && (
                              <div style={{
                                fontSize: 12, color: T.t2, lineHeight: 1.5,
                                padding: "8px 12px", borderRadius: T.r1,
                                background: T.bg3, borderLeft: `3px solid ${sevColor}`,
                              }}>
                                <span style={{ color: T.t3, fontFamily: T.fontMono, textTransform: "uppercase", fontSize: 9, letterSpacing: "0.06em", marginRight: 8 }}>→ Recommendation:</span>
                                {f.recommendation}
                              </div>
                            )}
                            {f.affected_entities?.length > 0 && (
                              <div style={{ marginTop: 8, display: "flex", flexWrap: "wrap", gap: 4 }}>
                                {f.affected_entities.slice(0, 6).map((e, j) => (
                                  <span key={j} style={{
                                    fontSize: 10, fontFamily: T.fontMono, color: T.t2,
                                    padding: "2px 6px", borderRadius: 3,
                                    background: T.bg3, border: `1px solid ${T.border0}`,
                                  }}>{e}</span>
                                ))}
                                {f.affected_entities.length > 6 && (
                                  <span style={{ fontSize: 10, color: T.t3, padding: "2px 6px" }}>
                                    +{f.affected_entities.length - 6} more
                                  </span>
                                )}
                              </div>
                            )}
                          </div>
                        </Card>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* CONSTRAINT VIOLATIONS — engineering rules, not LLM-judged */}
              {result.constraint_violations?.length > 0 && (
                <div>
                  <SectionTitle count={result.constraint_violations.length} delay={0.4}>Constraint Engine Violations</SectionTitle>
                  <p style={{ fontSize: 11, color: T.t3, marginBottom: 12, marginTop: -8 }}>
                    Deterministic rule-based checks (1-QM-per-app, sender/receiver pairs, channel naming, XMITQ existence, orphan QMs, consumer queues, path completeness, V-009 reachability).
                  </p>
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {result.constraint_violations.map((v, i) => (
                      <ViolationBadge key={i} v={v} delay={0.45 + 0.03 * i} />
                    ))}
                  </div>
                </div>
              )}

              {/* If no audit and no violations */}
              {!result.compliance_audit && (!result.constraint_violations || result.constraint_violations.length === 0) && (
                <EmptyState icon="✓" message="No compliance data available." />
              )}

              {/* Summary blurb */}
              {result.compliance_audit?.summary && (
                <Card delay={0.5}>
                  <CardHeader>Auditor Summary</CardHeader>
                  <div style={{ padding: "12px 16px", fontSize: 12, color: T.t2, lineHeight: 1.6, fontStyle: "italic" }}>
                    "{result.compliance_audit.summary}"
                  </div>
                </Card>
              )}
            </div>
          )}

          {tab === "trace" && result && !loading && (
            <div style={{ animation: "fadeUp 0.4s ease-out" }}>
              <div style={{ marginBottom: 20 }}>
                <h2 style={{ fontSize: 16, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1, marginBottom: 4 }}>
                  Agent Execution Trace
                </h2>
                <p style={{ fontSize: 12, color: T.t3 }}>Ordered log from every agent in the pipeline.</p>
              </div>
              {result.agent_trace?.length > 0 ? (
                <div style={{ position: "relative" }}>
                  {/* Timeline line */}
                  <div style={{
                    position: "absolute", left: 71, top: 0, bottom: 0,
                    width: 1, background: T.border0,
                  }} />
                  {result.agent_trace.map((m, i) => {
                    // Handle both {agent, msg} objects and plain strings
                    const agent = typeof m === "string" ? "Pipeline" : (m.agent || "Pipeline");
                    const msg = typeof m === "string" ? m : (m.msg || m.message || JSON.stringify(m));
                    return (
                      <div key={i} style={{
                        display: "flex", gap: 14, alignItems: "flex-start",
                        padding: "10px 0",
                        animation: `slideIn 0.3s ease-out ${0.04 * i}s both`,
                        position: "relative",
                      }}>
                        <span style={{
                          fontSize: 10, fontWeight: 600, fontFamily: T.fontMono,
                          color: T.cyan, minWidth: 64, textAlign: "right",
                          paddingTop: 2, flexShrink: 0,
                        }}>{agent}</span>
                        {/* Dot */}
                        <div style={{
                          width: 9, height: 9, borderRadius: "50%",
                          background: T.cyan, border: `2px solid ${T.bg0}`,
                          marginTop: 4, flexShrink: 0, zIndex: 1,
                        }} />
                        <div style={{
                          fontSize: 12, color: T.t2, lineHeight: 1.6,
                          padding: "6px 12px", background: T.bg2,
                          borderRadius: T.r1, border: `1px solid ${T.border0}`,
                          flex: 1,
                        }}>{msg}</div>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <EmptyState icon="⋯" message="No trace data available." />
              )}
            </div>
          )}

          {/* ── No result placeholder ── */}
          {!result && tab !== "upload" && !loading && (
            <EmptyState icon="◇" message="No analysis run yet. Go to the Upload tab and upload your MQ Raw Data file." />
          )}
        </div>
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   REVIEW CHAT PANEL — Chat with the Architect AI before deciding
   ═══════════════════════════════════════════════════════════════════════════ */

function ReviewChatPanel({ result, architectMethod, reviewLoading, onApprove, onRevise, onAbort, sessionId }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const chatEndRef = useRef(null);
  const inputRef = useRef(null);

  // Auto-scroll chat to bottom
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Add initial architect greeting
  useEffect(() => {
    const method = architectMethod === "llm" ? "AI reasoning" : "rule-based analysis";
    const score = result?.target_metrics?.total_score || "?";
    const reduction = result?.complexity_reduction?.reduction_pct || "?";
    const adrCount = result?.adrs?.length || 0;
    setMessages([{
      role: "assistant",
      content: `I've completed the topology redesign using ${method}. `
        + `Target complexity score: ${score}/100 (${reduction}% reduction). `
        + `I made ${adrCount} architecture decisions. `
        + `Ask me anything about the design before you approve or request changes.`,
    }]);
  }, [result, architectMethod]);

  async function sendMessage() {
    const text = input.trim();
    if (!text || chatLoading) return;

    const userMsg = { role: "user", content: text };
    setMessages(prev => [...prev, userMsg]);
    setInput("");
    setChatLoading(true);

    try {
      const res = await fetch(`${API}/api/chat/${sessionId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: text,
          history: messages.filter(m => m.role === "user" || m.role === "assistant"),
        }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setMessages(prev => [...prev, { role: "assistant", content: data.reply }]);
    } catch (e) {
      setMessages(prev => [...prev, {
        role: "assistant",
        content: "I couldn't process that request. You can still approve, revise, or abort using the buttons below.",
      }]);
    } finally {
      setChatLoading(false);
      inputRef.current?.focus();
    }
  }

  function handleRevise() {
    // Collect ALL chat messages (user + assistant) as structured context
    // The assistant's agreements/recommendations are critical for the revision
    const userMessages = messages
      .filter(m => m.role === "user")
      .map(m => m.content);
    const feedback = userMessages.length > 0
      ? userMessages.join("\n")
      : "Please revise the design.";
    // Pass full structured chat history alongside the flat feedback string
    const chatHistory = messages
      .filter(m => m.role === "user" || m.role === "assistant")
      .map(m => ({ role: m.role, content: m.content }));
    onRevise(feedback, chatHistory);
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* TOP ROW — Scores + Badges */}
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, flex: 1 }}>
          {[
            { label: "Complexity (As-Is)", value: result.as_is_metrics?.total_score?.toFixed?.(1) ?? result.as_is_metrics?.total_score, color: T.red },
            { label: "Complexity (Target)", value: result.target_metrics?.total_score?.toFixed?.(1) ?? result.target_metrics?.total_score, color: T.green },
            { label: "Complexity Reduction", value: `${result.complexity_reduction?.reduction_pct ?? "?"}%`, color: T.cyan },
            (() => {
              const sr = result.solver_run;
              const asis = sr?.asis_channel_count;
              const sol = sr?.actual_channel_count ?? sr?.final_channel_count;
              const pct = sr?.delta_pct_vs_asis;
              if (asis != null && sol != null && pct != null) {
                const reduction = -pct;
                return {
                  label: "Channels Reduced",
                  value: `${reduction.toFixed(1)}%`,
                  sub: `${asis} → ${sol}`,
                  color: T.cyan,
                };
              }
              return { label: "Channels", value: "—", sub: "no solver data", color: T.t3 };
            })(),
          ].map((s, i) => (
            <Card key={i} delay={0.05 * i}>
              <div style={{ padding: "10px 8px", textAlign: "center" }}>
                <div style={{ fontSize: 22, fontWeight: 700, fontFamily: T.fontDisplay, color: s.color }}>{s.value}</div>
                {s.sub && <div style={{ fontSize: 9, color: T.t3, marginTop: 2, fontFamily: T.fontMono }}>{s.sub}</div>}
                <div style={{ fontSize: 9, color: T.t3, marginTop: 2, fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.06em" }}>{s.label}</div>
              </div>
            </Card>
          ))}
        </div>
        <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
          {result.redesign_count > 1 && <Badge color={T.amber} style={{ fontSize: 9 }}>Iteration {result.redesign_count}</Badge>}
          {architectMethod && (
            <Badge color={
              architectMethod === "revision_architect" ? T.purple
              : architectMethod === "hybrid_cluster" ? T.cyan
              : architectMethod === "llm" ? T.cyan
              : T.amber
            } style={{ fontSize: 9 }}>
              {architectMethod === "revision_architect" ? "◆ AI REVISION"
               : architectMethod === "hybrid_cluster" ? "◆ AI HYBRID"
               : architectMethod === "llm" ? "◆ AI ARCHITECT"
               : "◇ RULES ENGINE"}
            </Badge>
          )}
        </div>
      </div>

      {/* TOPOLOGY — Always visible */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <TopologyGraph graphData={result.as_is_graph} title="As-Is Topology" height={360} />
        <TopologyGraph graphData={result.target_graph} title="Proposed Target" height={360}
          badge={<Badge color={T.green} style={{ fontSize: 8 }}>NEW</Badge>} />
      </div>

      {/* MAIN ROW — ADRs + Actions (left 60%) | Chat (right 40%) */}
      <div style={{ display: "grid", gridTemplateColumns: "3fr 2fr", gap: 14 }}>

        {/* LEFT — ADRs + Action buttons */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {/* ADR summary */}
          {result.adrs?.length > 0 && (
            <Card delay={0.1}>
              <CardHeader right={<Badge color={T.cyan}>{result.adrs.length}</Badge>}>Architecture Decisions</CardHeader>
              <div style={{ padding: "8px 12px", maxHeight: 180, overflowY: "auto" }}>
                {result.adrs.map((adr, i) => (
                  <div key={i} style={{
                    padding: "6px 0", borderBottom: i < result.adrs.length - 1 ? `1px solid ${T.border0}` : "none",
                    display: "flex", alignItems: "flex-start", gap: 8,
                  }}>
                    <Badge color={T.cyan} style={{ fontSize: 8, flexShrink: 0, marginTop: 2 }}>{adr.id}</Badge>
                    <span style={{ fontSize: 11, color: T.t2, lineHeight: 1.4 }}>{adr.decision || adr.title}</span>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {/* Violations if any */}
          {result.constraint_violations?.length > 0 && (
            <div>
              {result.constraint_violations.slice(0, 3).map((v, i) => (
                <ViolationBadge key={i} v={v} delay={0.05 * i} />
              ))}
            </div>
          )}

          {/* Action buttons */}
          <div style={{ marginTop: "auto" }}>
            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={onApprove} disabled={reviewLoading} style={{
                flex: 2, padding: "12px 16px", borderRadius: T.r1, border: "none",
                background: `linear-gradient(180deg, ${T.green}, ${T.greenDim})`,
                color: "#fff", fontSize: 13, fontWeight: 600, cursor: reviewLoading ? "not-allowed" : "pointer",
                opacity: reviewLoading ? 0.6 : 1, fontFamily: T.fontSans,
                boxShadow: `0 2px 12px ${T.green}40`, transition: "all 0.15s",
              }}>
                {reviewLoading ? "Processing..." : "✓ Approve — Generate Outputs"}
              </button>
              <button onClick={handleRevise} disabled={reviewLoading || messages.filter(m => m.role === "user").length === 0} style={{
                flex: 1, padding: "12px 16px", borderRadius: T.r1,
                border: `1px solid ${T.cyan}50`, background: T.cyanBg,
                color: T.cyan, fontSize: 13, fontWeight: 600,
                cursor: (reviewLoading || messages.filter(m => m.role === "user").length === 0) ? "not-allowed" : "pointer",
                opacity: (reviewLoading || messages.filter(m => m.role === "user").length === 0) ? 0.4 : 1,
                fontFamily: T.fontSans, transition: "all 0.15s",
              }}>
                {reviewLoading ? "..." : "↻ Revise with Feedback"}
              </button>
              <button onClick={onAbort} disabled={reviewLoading} style={{
                padding: "12px 14px", borderRadius: T.r1,
                border: `1px solid ${T.red}30`, background: "transparent",
                color: T.red, fontSize: 11, fontWeight: 600,
                cursor: reviewLoading ? "not-allowed" : "pointer",
                opacity: reviewLoading ? 0.6 : 0.6, fontFamily: T.fontSans,
                transition: "all 0.15s",
              }}>
                ✗
              </button>
            </div>
            <div style={{ fontSize: 9, color: T.t4, marginTop: 6, lineHeight: 1.5 }}>
              Chat with the Architect first, then <strong style={{ color: T.green }}>Approve</strong> or <strong style={{ color: T.cyan }}>Revise</strong> (sends your chat as feedback).
            </div>
          </div>
        </div>

        {/* RIGHT — Compact Chat */}
        <Card glow={T.cyan} style={{ display: "flex", flexDirection: "column" }}>
          <CardHeader right={
            <span style={{ fontSize: 9, color: T.t4, fontFamily: T.fontMono }}>AI Chat</span>
          }>Ask the Architect</CardHeader>

          {/* Messages */}
          <div style={{
            flex: 1, overflowY: "auto", padding: "10px 12px",
            display: "flex", flexDirection: "column", gap: 8,
            minHeight: 200, maxHeight: 300,
          }}>
            {messages.map((msg, i) => (
              <div key={i} style={{
                display: "flex", flexDirection: "column",
                alignItems: msg.role === "user" ? "flex-end" : "flex-start",
                animation: "fadeUp 0.2s ease-out",
              }}>
                <div style={{
                  padding: "6px 10px", borderRadius: T.r1,
                  background: msg.role === "user" ? T.cyanBg : T.bg3,
                  border: `1px solid ${msg.role === "user" ? T.cyanBorder : T.border0}`,
                  fontSize: 11, color: T.t1, lineHeight: 1.5,
                  maxWidth: "92%",
                }}>
                  {msg.content}
                </div>
              </div>
            ))}
            {chatLoading && (
              <div style={{
                padding: "6px 10px", borderRadius: T.r1,
                background: T.bg3, border: `1px solid ${T.border0}`,
                fontSize: 11, color: T.t3, alignSelf: "flex-start",
              }}>
                <span style={{ animation: "pulse 1s infinite" }}>Thinking...</span>
              </div>
            )}
            <div ref={chatEndRef} />
          </div>

          {/* Input */}
          <div style={{
            padding: "8px 10px", borderTop: `1px solid ${T.border0}`,
            display: "flex", gap: 6,
          }}>
            <input
              ref={inputRef}
              type="text"
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMessage(); } }}
              placeholder="Ask about the design..."
              disabled={chatLoading || reviewLoading}
              style={{
                flex: 1, padding: "7px 10px", borderRadius: T.r1,
                border: `1px solid ${T.border1}`, fontSize: 11,
                background: T.bg1, color: T.t1, fontFamily: T.fontSans,
                outline: "none",
              }}
              onFocus={e => e.target.style.borderColor = T.cyan}
              onBlur={e => e.target.style.borderColor = T.border1}
            />
            <button onClick={sendMessage} disabled={chatLoading || !input.trim() || reviewLoading} style={{
              padding: "7px 12px", borderRadius: T.r1, border: "none",
              background: (chatLoading || !input.trim()) ? T.bg3 : T.cyan,
              color: (chatLoading || !input.trim()) ? T.t4 : T.bg0,
              fontSize: 11, fontWeight: 600, cursor: (chatLoading || !input.trim()) ? "not-allowed" : "pointer",
              transition: "all 0.15s",
            }}>
              →
            </button>
          </div>
        </Card>
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   PIPELINE DIAGRAM — Animated 10-agent flow
   ═══════════════════════════════════════════════════════════════════════════ */

const PIPELINE_AGENTS = [
  { id: 1,  name: "Supervisor",      icon: "⊡", desc: "Validate session & paths",       color: T.t3 },
  { id: 2,  name: "Sanitiser",       icon: "⊟", desc: "Clean & deduplicate data",       color: T.t3 },
  { id: 3,  name: "Researcher",      icon: "◇", desc: "Build graph, detect violations",  color: T.cyan },
  { id: 4,  name: "Analyst",         icon: "▤", desc: "5-factor complexity score",       color: T.cyan },
  { id: 5,  name: "Architect",       icon: "◆", desc: "LLM designs target state",       color: T.green },
  { id: 6,  name: "Optimizer",       icon: "⊘", desc: "Prune channels via reachability", color: T.green },
  { id: 7,  name: "Tester",          icon: "◎", desc: "8 constraint validation checks",  color: T.amber },
  { id: 8,  name: "Human Review",    icon: "⏸", desc: "Approve / reject / abort",       color: T.amber },
  { id: 9,  name: "Provisioner",     icon: "▸", desc: "Per-QM MQSC + target CSVs",      color: T.purple },
  { id: 10, name: "Migration",       icon: "⇄", desc: "4-phase rollback-safe plan",     color: T.purple },
];

function PipelineDiagram() {
  const [activeIdx, setActiveIdx] = useState(-1);

  useEffect(() => {
    // Sequentially light up each agent
    let i = 0;
    const timer = setInterval(() => {
      setActiveIdx(i);
      i++;
      if (i >= PIPELINE_AGENTS.length) {
        // Reset after a pause and loop
        setTimeout(() => {
          setActiveIdx(-1);
          i = 0;
        }, 1200);
      }
    }, 350);
    return () => clearInterval(timer);
  }, []);

  return (
    <div style={{
      marginTop: 28, padding: "20px 0",
      animation: "fadeUp 0.5s ease-out 0.4s both",
    }}>
      {/* Title */}
      <div style={{
        textAlign: "center", marginBottom: 16,
        fontSize: 10, fontWeight: 600, color: T.t4,
        fontFamily: T.fontMono, textTransform: "uppercase", letterSpacing: "0.1em",
      }}>
        10-Agent Pipeline
      </div>

      {/* Pipeline flow */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "center",
        gap: 0, overflowX: "auto", padding: "4px 8px",
      }}>
        {PIPELINE_AGENTS.map((agent, i) => {
          const isActive = i <= activeIdx;
          const isCurrent = i === activeIdx;
          return (
            <div key={agent.id} style={{ display: "flex", alignItems: "center", flexShrink: 0 }}>
              {/* Agent node */}
              <div style={{ position: "relative", textAlign: "center" }}>
                {/* Glow ring for current */}
                {isCurrent && (
                  <div style={{
                    position: "absolute", top: -4, left: "50%", transform: "translateX(-50%)",
                    width: 38, height: 38, borderRadius: "50%",
                    border: `2px solid ${agent.color}`,
                    opacity: 0.4,
                    animation: "pulse 1s infinite",
                  }} />
                )}
                {/* Circle */}
                <div style={{
                  width: 30, height: 30, borderRadius: "50%",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 12,
                  background: isActive ? `${agent.color}20` : T.bg2,
                  border: `1.5px solid ${isActive ? agent.color : T.border0}`,
                  color: isActive ? agent.color : T.t4,
                  transition: "all 0.3s cubic-bezier(0.16, 1, 0.3, 1)",
                  boxShadow: isCurrent ? `0 0 12px ${agent.color}30` : "none",
                }}>
                  {agent.icon}
                </div>
                {/* Label */}
                <div style={{
                  fontSize: 8, fontFamily: T.fontMono, fontWeight: 600,
                  color: isActive ? agent.color : T.t4,
                  marginTop: 5, whiteSpace: "nowrap",
                  transition: "color 0.3s",
                  letterSpacing: "0.02em",
                  maxWidth: 56, overflow: "hidden", textOverflow: "ellipsis",
                }}>
                  {agent.name}
                </div>
                {/* Description tooltip on current */}
                {isCurrent && (
                  <div style={{
                    position: "absolute", top: -32, left: "50%", transform: "translateX(-50%)",
                    padding: "3px 8px", borderRadius: 4,
                    background: agent.color, color: T.bg0,
                    fontSize: 8, fontFamily: T.fontMono, fontWeight: 600,
                    whiteSpace: "nowrap",
                    animation: "fadeIn 0.2s ease-out",
                    boxShadow: `0 2px 8px ${agent.color}40`,
                  }}>
                    {agent.desc}
                    {/* Arrow */}
                    <div style={{
                      position: "absolute", bottom: -4, left: "50%", transform: "translateX(-50%)",
                      width: 0, height: 0,
                      borderLeft: "4px solid transparent", borderRight: "4px solid transparent",
                      borderTop: `4px solid ${agent.color}`,
                    }} />
                  </div>
                )}
              </div>

              {/* Connector line */}
              {i < PIPELINE_AGENTS.length - 1 && (
                <div style={{
                  width: 20, height: 1.5,
                  background: i < activeIdx
                    ? `linear-gradient(90deg, ${PIPELINE_AGENTS[i].color}80, ${PIPELINE_AGENTS[i + 1].color}80)`
                    : T.border0,
                  transition: "background 0.3s",
                  margin: "0 1px",
                  marginBottom: 18, /* align with circles, not labels */
                }} />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   UPLOAD TAB — Hero demo launcher + collapsible custom upload
   ═══════════════════════════════════════════════════════════════════════════ */

function UploadTab({ runDemo, handleUpload }) {
  const [dragOver, setDragOver] = useState(false);
  const fileInputRef = useRef(null);

  function handleDrop(e) {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer?.files?.[0];
    if (file && fileInputRef.current) {
      // Create a new DataTransfer to set the file input
      const dt = new DataTransfer();
      dt.items.add(file);
      fileInputRef.current.files = dt.files;
    }
  }

  return (
    <div style={{ animation: "fadeUp 0.4s ease-out", maxWidth: 640, margin: "0 auto" }}>
      {/* Hero section */}
      <div style={{ textAlign: "center", marginBottom: 48, marginTop: 24 }}>
        <div style={{
          width: 64, height: 64, borderRadius: 16, margin: "0 auto 20px",
          background: `linear-gradient(135deg, ${T.cyan}18, ${T.cyan}05)`,
          border: `1px solid ${T.cyan}25`,
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: 28, color: T.cyan,
          boxShadow: `0 0 40px ${T.cyan}15, 0 0 80px ${T.cyan}08`,
          animation: "scaleIn 0.5s ease-out",
        }}>◆</div>
        <h2 style={{
          fontSize: 28, fontWeight: 700, fontFamily: T.fontDisplay, color: T.t1,
          marginBottom: 10, letterSpacing: "-0.01em",
          animation: "fadeUp 0.5s ease-out 0.1s both",
        }}>
          Intelligent MQ Topology
        </h2>
        <p style={{
          fontSize: 14, color: T.t3, lineHeight: 1.7, maxWidth: 460, margin: "0 auto",
          animation: "fadeUp 0.5s ease-out 0.2s both",
        }}>
          10 AI agents analyse, redesign, and provision your IBM MQ infrastructure.
          Upload your MQ Raw Data export to begin.
        </p>
      </div>

      {/* Upload area — single file */}
      <div style={{ animation: "fadeUp 0.5s ease-out 0.3s both" }}>
        <Card glow={dragOver ? T.cyan : undefined} style={{ marginBottom: 20 }}>
          <CardHeader right={
            <span style={{ fontSize: 9, color: T.t4, fontFamily: T.fontMono }}>.csv / .xlsx</span>
          }>Upload MQ Raw Data</CardHeader>
          <form onSubmit={handleUpload}>
            <div
              onDragOver={e => { e.preventDefault(); setDragOver(true); }}
              onDragLeave={() => setDragOver(false)}
              onDrop={handleDrop}
              style={{
                padding: "36px 24px", textAlign: "center",
                borderBottom: `1px solid ${T.border0}`,
                background: dragOver ? `${T.cyan}08` : "transparent",
                transition: "background 0.2s",
              }}
            >
              <div style={{
                width: 52, height: 52, borderRadius: 12, margin: "0 auto 16px",
                background: `${T.cyan}10`, border: `1.5px dashed ${dragOver ? T.cyan : T.border2}`,
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 22, color: dragOver ? T.cyan : T.t3,
                transition: "all 0.2s",
              }}>⬆</div>
              <div style={{ fontSize: 13, color: T.t2, marginBottom: 8 }}>
                Drop your <strong style={{ color: T.t1 }}>MQ Raw Data</strong> file here
              </div>
              <div style={{ fontSize: 11, color: T.t4, marginBottom: 16 }}>
                or click to browse — supports .csv and .xlsx
              </div>
              <div style={{ position: "relative", display: "inline-block" }}>
                <input
                  ref={fileInputRef}
                  type="file"
                  name="mq_raw_data"
                  accept=".csv,.xlsx,.xls"
                  required
                  style={{
                    position: "absolute", inset: 0, opacity: 0, cursor: "pointer", width: "100%", height: "100%",
                  }}
                  onChange={e => {
                    // Force re-render to show filename
                    const fname = e.target.files?.[0]?.name;
                    if (fname) e.target.closest("form").querySelector("[data-filename]").textContent = fname;
                  }}
                />
                <div style={{
                  padding: "8px 20px", borderRadius: T.r1,
                  border: `1px solid ${T.border1}`,
                  background: T.bg3, color: T.t2,
                  fontSize: 11, fontFamily: T.fontMono,
                  cursor: "pointer",
                }}>
                  <span data-filename="">Choose file...</span>
                </div>
              </div>
            </div>
            <div style={{ padding: "14px 16px" }}>
              <button type="submit" style={{
                width: "100%", padding: "14px",
                borderRadius: T.r1, border: "none",
                background: `linear-gradient(180deg, ${T.green}, ${T.greenDim})`,
                color: "#fff", fontSize: 14, fontWeight: 600,
                cursor: "pointer", fontFamily: T.fontSans,
                boxShadow: `0 2px 12px ${T.green}40`,
                transition: "all 0.15s",
              }}
                onMouseEnter={e => { e.currentTarget.style.transform = "translateY(-1px)"; e.currentTarget.style.boxShadow = `0 4px 20px ${T.green}50`; }}
                onMouseLeave={e => { e.currentTarget.style.transform = "translateY(0)"; e.currentTarget.style.boxShadow = `0 2px 12px ${T.green}40`; }}
              >
                ▶ Analyse My MQ Environment
              </button>
            </div>
          </form>
        </Card>

        {/* Data format info */}
        <Card delay={0.1} style={{ marginBottom: 20 }}>
          <CardHeader>Expected Data Format</CardHeader>
          <div style={{ padding: "14px 16px", fontSize: 11, color: T.t3, lineHeight: 1.7 }}>
            <div style={{ marginBottom: 10 }}>
              Single Excel or CSV file with MQ topology rows. Each row = one <strong style={{ color: T.t2 }}>app→queue</strong> relationship. Required columns:
            </div>
            <div style={{
              display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 16px",
              fontFamily: T.fontMono, fontSize: 10,
            }}>
              {[
                ["queue_manager_name", "QM hosting the queue"],
                ["app_id", "Application identifier"],
                ["Discrete Queue Name", "Queue name"],
                ["PrimaryAppRole", "Producer / Consumer"],
                ["q_type", "Local / Remote / Alias"],
                ["remote_q_mgr_name", "Target QM (remote)"],
                ["remote_q_name", "Target queue (remote)"],
                ["xmit_q_name", "Transmission queue"],
                ["Neighborhood", "Region / LOB"],
                ["line_of_business", "Business unit"],
              ].map(([col, desc]) => (
                <div key={col} style={{ padding: "3px 0", display: "flex", gap: 6 }}>
                  <span style={{ color: T.cyan, fontWeight: 600, minWidth: 140 }}>{col}</span>
                  <span style={{ color: T.t4 }}>{desc}</span>
                </div>
              ))}
            </div>
          </div>
        </Card>
      </div>

      {/* Animated pipeline diagram */}
      <PipelineDiagram />

      {/* Demo fallback — small and secondary */}
      <div style={{ marginTop: 24, animation: "fadeUp 0.5s ease-out 0.5s both" }}>
        <button onClick={runDemo} style={{
          width: "100%", padding: "12px 16px",
          borderRadius: T.r1,
          background: "transparent",
          border: `1px solid ${T.border0}`,
          cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
          color: T.t4, fontSize: 11, fontFamily: T.fontMono,
          transition: "all 0.15s",
        }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = T.border1; e.currentTarget.style.color = T.t3; }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = T.border0; e.currentTarget.style.color = T.t4; }}
        >
          ▸ Or run with bundled demo data (if available)
        </button>
      </div>
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   REPORT VIEWER — Splits report into collapsible sections to prevent freeze
   ═══════════════════════════════════════════════════════════════════════════ */

function ReportViewer({ report, downloadFile }) {
  const sections = useMemo(() => {
    if (!report) return [];
    const lines = report.split("\n");
    const result = [];
    let current = { title: "Executive Summary", lines: [] };
    lines.forEach(line => {
      if (line.startsWith("## ") && current.lines.length > 0) {
        result.push(current);
        current = { title: line.replace(/^##\s*/, ""), lines: [] };
      } else {
        current.lines.push(line);
      }
    });
    if (current.lines.length > 0) result.push(current);
    return result;
  }, [report]);

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h2 style={{ fontSize: 16, fontWeight: 600, fontFamily: T.fontDisplay, color: T.t1 }}>Final Analysis Report</h2>
        <DownloadButton label="Download Full Report" onClick={() => downloadFile(report, "intelli_ai_report.md")} />
      </div>
      {sections.map((section, i) => (
        <ReportSection key={i} title={section.title} lines={section.lines}
          defaultOpen={!/violation|trace|migration commands/i.test(section.title)}
          delay={0.04 * i} />
      ))}
    </>
  );
}

function ReportSection({ title, lines, defaultOpen = true, delay = 0 }) {
  const [open, setOpen] = useState(defaultOpen);
  const [showAll, setShowAll] = useState(lines.length <= 60);
  const display = showAll ? lines.join("\n") : lines.slice(0, 60).join("\n") + `\n\n... (${lines.length - 60} more lines)`;

  return (
    <Card style={{ marginBottom: 10 }} delay={delay}>
      <div onClick={() => setOpen(!open)} style={{
        padding: "12px 16px", cursor: "pointer",
        display: "flex", alignItems: "center", gap: 10,
        background: open ? `${T.bg3}40` : "transparent",
      }}
        onMouseEnter={e => { if (!open) e.currentTarget.style.background = `${T.bg3}20`; }}
        onMouseLeave={e => { if (!open) e.currentTarget.style.background = open ? `${T.bg3}40` : "transparent"; }}
      >
        <span style={{ fontSize: 13, fontWeight: 600, color: T.t1, flex: 1 }}>{title}</span>
        <Badge color={T.t3} style={{ fontSize: 9 }}>{lines.length} lines</Badge>
        <span style={{ fontSize: 10, color: T.t4, transform: open ? "rotate(180deg)" : "rotate(0deg)", transition: "transform 0.2s" }}>▼</span>
      </div>
      {open && (
        <div style={{ borderTop: `1px solid ${T.border0}` }}>
          <pre style={{
            padding: "16px 20px", margin: 0, fontSize: 12, lineHeight: 1.7,
            fontFamily: T.fontSans, color: T.t2, overflowX: "auto",
            whiteSpace: "pre-wrap", wordBreak: "break-word",
          }}>{display}</pre>
          {!showAll && (
            <div style={{ padding: "8px 16px 12px", borderTop: `1px solid ${T.border0}` }}>
              <button onClick={e => { e.stopPropagation(); setShowAll(true); }} style={{
                padding: "6px 14px", borderRadius: T.r1, fontSize: 11,
                background: T.cyanBg, border: `1px solid ${T.cyanBorder}`,
                color: T.cyan, cursor: "pointer", fontFamily: T.fontMono,
              }}>Show all {lines.length} lines</button>
            </div>
          )}
        </div>
      )}
    </Card>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════
   MIGRATION STEP — Expandable
   ═══════════════════════════════════════════════════════════════════════════ */

function MigrationStep({ step, delay = 0 }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{
      borderBottom: `1px solid ${T.border0}`,
      animation: `slideIn 0.3s ease-out ${delay}s both`,
    }}>
      <div onClick={() => setOpen(!open)} style={{
        padding: "12px 16px", cursor: "pointer",
        display: "flex", alignItems: "center", gap: 12,
        transition: "background 0.15s",
        background: open ? `${T.bg3}60` : "transparent",
      }}
        onMouseEnter={e => { if (!open) e.currentTarget.style.background = `${T.bg3}30`; }}
        onMouseLeave={e => { if (!open) e.currentTarget.style.background = "transparent"; }}
      >
        <span style={{
          fontSize: 11, fontWeight: 700, fontFamily: T.fontMono,
          color: T.t3, minWidth: 24,
        }}>{step.step_number}</span>
        <PhaseBadge phase={step.phase} />
        <span style={{ fontSize: 12, color: T.t2, flex: 1 }}>{step.description}</span>
        {step.target_qm && <Badge color={T.t3} style={{ fontSize: 9 }}>{step.target_qm}</Badge>}
        <span style={{
          fontSize: 10, color: T.t4,
          transform: open ? "rotate(180deg)" : "rotate(0deg)",
          transition: "transform 0.2s",
        }}>▼</span>
      </div>
      {open && (
        <div style={{ padding: "0 16px 16px 52px", animation: "fadeIn 0.2s" }}>
          {step.depends_on?.length > 0 && (
            <div style={{ fontSize: 11, color: T.t3, marginBottom: 10, fontFamily: T.fontMono }}>
              Depends on: {step.depends_on.map(d => `Step ${d}`).join(", ")}
            </div>
          )}
          {[
            ["Forward MQSC", step.mqsc_forward, T.green],
            ["Rollback MQSC", step.mqsc_rollback, T.red],
            ["Verification", step.verification, T.cyan],
          ].map(([label, content, color]) => content && (
            <div key={label} style={{ marginBottom: 10 }}>
              <div style={{
                fontSize: 10, fontWeight: 600, color, fontFamily: T.fontMono,
                textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 4,
              }}>{label}</div>
              <pre style={{
                padding: "10px 12px", borderRadius: T.r1,
                background: T.bg1, border: `1px solid ${T.border0}`,
                fontSize: 10, fontFamily: T.fontMono, color: T.t2,
                lineHeight: 1.6, overflowX: "auto", margin: 0,
                whiteSpace: "pre-wrap",
              }}>{content}</pre>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
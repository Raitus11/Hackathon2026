import { useState, useEffect, useRef } from "react";
import * as d3 from "d3";

const API = "http://localhost:8000";

// ── Colour map by node type ──────────────────────────────────────────────
const NODE_COLOR = {
  qm:    "#185FA5",
  app:   "#1D9E75",
  queue: "#BA7517",
};

// ── D3 Force Graph Component ─────────────────────────────────────────────
function TopologyGraph({ graphData, title, height = 340 }) {
  const svgRef = useRef(null);

  useEffect(() => {
    if (!graphData?.nodes?.length) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const w = svgRef.current.clientWidth || 600;
    const h = height;

    const sim = d3.forceSimulation(graphData.nodes)
      .force("link",   d3.forceLink(graphData.edges).id(d => d.id).distance(90))
      .force("charge", d3.forceManyBody().strength(-300))
      .force("center", d3.forceCenter(w / 2, h / 2))
      .force("collide", d3.forceCollide(30));

    const g = svg.append("g");

    svg.call(d3.zoom().scaleExtent([0.3, 3]).on("zoom", e => g.attr("transform", e.transform)));

    // Arrow markers
    svg.append("defs").selectAll("marker")
      .data(["channel", "connects_to", "owns"])
      .join("marker")
      .attr("id", d => `arrow-${d}`)
      .attr("viewBox", "0 0 10 10")
      .attr("refX", 22).attr("refY", 5)
      .attr("markerWidth", 6).attr("markerHeight", 6)
      .attr("orient", "auto-start-reverse")
      .append("path")
      .attr("d", "M2 1L8 5L2 9")
      .attr("fill", "none")
      .attr("stroke", d => d === "channel" ? "#185FA5" : d === "connects_to" ? "#1D9E75" : "#888")
      .attr("stroke-width", 1.5);

    // Links
    const link = g.selectAll("line")
      .data(graphData.edges.filter(e => e.rel === "channel" || e.rel === "connects_to"))
      .join("line")
      .attr("stroke", d => d.rel === "channel" ? "#185FA5" : "#1D9E75")
      .attr("stroke-width", d => d.rel === "channel" ? 1.5 : 0.8)
      .attr("stroke-dasharray", d => d.rel === "connects_to" ? "4 2" : null)
      .attr("stroke-opacity", 0.7)
      .attr("marker-end", d => `url(#arrow-${d.rel})`);

    // Nodes
    const node = g.selectAll("g.node")
      .data(graphData.nodes.filter(n => n.type === "qm" || n.type === "app"))
      .join("g")
      .attr("class", "node")
      .style("cursor", "pointer")
      .call(d3.drag()
        .on("start", (e, d) => { if (!e.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on("drag",  (e, d) => { d.fx = e.x; d.fy = e.y; })
        .on("end",   (e, d) => { if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; })
      );

    node.append("circle")
      .attr("r", d => d.type === "qm" ? 20 : 14)
      .attr("fill", d => NODE_COLOR[d.type] || "#888")
      .attr("stroke", "#fff")
      .attr("stroke-width", 2);

    node.append("text")
      .text(d => d.type === "qm" ? "QM" : "APP")
      .attr("text-anchor", "middle")
      .attr("dy", "0.35em")
      .attr("fill", "#fff")
      .attr("font-size", "9px")
      .attr("font-weight", "600");

    node.append("text")
      .text(d => (d.name || d.id).replace("QM_", "").replace("APP_", "").slice(0, 14))
      .attr("text-anchor", "middle")
      .attr("dy", "2.4em")
      .attr("fill", "currentColor")
      .attr("font-size", "9px");

    node.append("title").text(d => `${d.id}\nType: ${d.type}\nRegion: ${d.region || "-"}`);

    sim.on("tick", () => {
      link
        .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
      node.attr("transform", d => `translate(${d.x},${d.y})`);
    });
  }, [graphData, height]);

  return (
    <div style={{ background: "var(--color-background-secondary)", borderRadius: 8, padding: "8px 12px" }}>
      <p style={{ margin: "0 0 6px", fontSize: 13, fontWeight: 500, color: "var(--color-text-secondary)" }}>{title}</p>
      <svg ref={svgRef} width="100%" height={height} style={{ display: "block" }} />
      <div style={{ display: "flex", gap: 16, fontSize: 11, color: "var(--color-text-tertiary)", marginTop: 4 }}>
        <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 10, height: 10, borderRadius: "50%", background: NODE_COLOR.qm, display: "inline-block" }}/> Queue Manager</span>
        <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 10, height: 10, borderRadius: "50%", background: NODE_COLOR.app, display: "inline-block" }}/> Application</span>
      </div>
    </div>
  );
}

// ── Metrics Card ─────────────────────────────────────────────────────────
function MetricRow({ label, before, after }) {
  const improved = after < before;
  const delta = before - after;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 80px 80px 80px", gap: 8, padding: "8px 0", borderBottom: "1px solid var(--color-border-tertiary)", alignItems: "center", fontSize: 13 }}>
      <span style={{ color: "var(--color-text-primary)" }}>{label}</span>
      <span style={{ textAlign: "right", color: "var(--color-text-secondary)" }}>{before}</span>
      <span style={{ textAlign: "right", fontWeight: 500, color: improved ? "var(--color-text-success)" : "var(--color-text-primary)" }}>{after}</span>
      <span style={{ textAlign: "right", fontSize: 11, color: improved ? "var(--color-text-success)" : "var(--color-text-tertiary)" }}>
        {improved ? `↓ ${delta}` : delta === 0 ? "—" : `↑ ${Math.abs(delta)}`}
      </span>
    </div>
  );
}

// ── Score Gauge ───────────────────────────────────────────────────────────
function ScoreGauge({ label, score, max = 100 }) {
  const pct = Math.min((score / max) * 100, 100);
  const color = score < 40 ? "#1D9E75" : score < 70 ? "#BA7517" : "#E24B4A";
  return (
    <div style={{ textAlign: "center" }}>
      <div style={{ fontSize: 28, fontWeight: 600, color }}>{score}</div>
      <div style={{ height: 6, background: "var(--color-border-tertiary)", borderRadius: 3, margin: "4px 0" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 3, transition: "width 0.6s" }} />
      </div>
      <div style={{ fontSize: 11, color: "var(--color-text-tertiary)" }}>{label}</div>
    </div>
  );
}

// ── ADR Card ──────────────────────────────────────────────────────────────
function ADRCard({ adr }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ border: "1px solid var(--color-border-tertiary)", borderRadius: 8, overflow: "hidden", marginBottom: 8 }}>
      <div onClick={() => setOpen(!open)} style={{ padding: "10px 14px", cursor: "pointer", display: "flex", justifyContent: "space-between", background: "var(--color-background-secondary)" }}>
        <span style={{ fontSize: 13, fontWeight: 500 }}>{adr.id} — {adr.decision}</span>
        <span style={{ fontSize: 11, color: "var(--color-text-tertiary)" }}>{open ? "▲" : "▼"}</span>
      </div>
      {open && (
        <div style={{ padding: "10px 14px", fontSize: 12, color: "var(--color-text-secondary)", lineHeight: 1.6 }}>
          <p><strong>Context:</strong> {adr.context}</p>
          <p><strong>Rationale:</strong> {adr.rationale}</p>
          <p><strong>Consequences:</strong> {adr.consequences}</p>
        </div>
      )}
    </div>
  );
}

// ── Violation Badge ───────────────────────────────────────────────────────
function ViolationBadge({ v }) {
  const bg = v.severity === "CRITICAL" ? "var(--color-background-danger)" : "var(--color-background-warning)";
  const color = v.severity === "CRITICAL" ? "var(--color-text-danger)" : "var(--color-text-warning)";
  return (
    <div style={{ padding: "8px 12px", borderRadius: 6, marginBottom: 6, background: bg, fontSize: 12 }}>
      <span style={{ fontWeight: 600, color }}>[{v.severity}] {v.rule}</span>
      <span style={{ color: "var(--color-text-secondary)", marginLeft: 8 }}>{v.entity} — {v.detail}</span>
    </div>
  );
}

// ── Main App ──────────────────────────────────────────────────────────────
export default function App() {
  const [tab, setTab] = useState("upload");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [reviewFeedback, setReviewFeedback] = useState("");
  const [reviewLoading, setReviewLoading] = useState(false);

  async function submitReview(approved) {
    if (!approved && !reviewFeedback.trim()) {
      setError("Please provide a reason for rejection.");
      return;
    }
    setReviewLoading(true);
    setError(null);
    try {
      const sessionId = result?.session_id;
      const res = await fetch(`${API}/api/review/${sessionId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ approved, feedback: reviewFeedback }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setResult(data);
      setReviewFeedback("");
      // After approval go to topology, after rejection stay on review
      if (approved) setTab("topology");
    } catch (e) {
      setError(e.message);
    } finally {
      setReviewLoading(false);
    }
  }

  const tabs = ["upload", "review", "topology", "metrics", "adrs", "mqsc", "csvs", "trace"];

  async function runDemo() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}/api/demo`, { method: "POST" });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setResult(data);
      setTab(data.awaiting_human_review ? "review" : "topology");
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  async function handleUpload(e) {
    e.preventDefault();
    const form = new FormData(e.target);
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}/api/analyse`, { method: "POST", body: form });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setResult(data);
      setTab(data.awaiting_human_review ? "review" : "topology");
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 20px", fontFamily: "var(--font-sans)" }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 600, margin: "0 0 4px", color: "var(--color-text-primary)" }}>MQ-TITAN</h1>
        <p style={{ fontSize: 13, color: "var(--color-text-secondary)", margin: 0 }}>MQ Topology Intelligence & Transformation Agent Network</p>
      </div>

      {/* Tab bar */}
      <div style={{ display: "flex", gap: 4, marginBottom: 20, borderBottom: "1px solid var(--color-border-tertiary)", paddingBottom: 0 }}>
        {tabs.map(t => (
          <button key={t} onClick={() => setTab(t)} style={{
            padding: "7px 14px", fontSize: 12, fontWeight: 500, border: "none", cursor: "pointer",
            background: tab === t ? "var(--color-background-primary)" : "transparent",
            borderBottom: tab === t ? "2px solid var(--color-text-info)" : "2px solid transparent",
            color: tab === t ? "var(--color-text-info)" : "var(--color-text-secondary)",
            textTransform: "capitalize",
          }}>
            {t}
            {t === "adrs" && result?.adrs?.length ? ` (${result.adrs.length})` : ""}
            {t === "topology" && result ? " ✓" : ""}
          </button>
        ))}
      </div>

      {/* Error */}
      {error && (
        <div style={{ background: "var(--color-background-danger)", color: "var(--color-text-danger)", padding: "10px 14px", borderRadius: 8, marginBottom: 16, fontSize: 13 }}>
          {error}
        </div>
      )}

      {/* Loading */}
      {loading && (
        <div style={{ padding: "40px 0", textAlign: "center", color: "var(--color-text-secondary)", fontSize: 13 }}>
          Running 9-agent pipeline... this may take 10-30 seconds.
        </div>
      )}

      {/* ── UPLOAD TAB ── */}
      {tab === "upload" && !loading && (
        <div style={{ maxWidth: 500 }}>
          <p style={{ fontSize: 13, color: "var(--color-text-secondary)", marginBottom: 20 }}>
            Upload 4 CSV files representing your MQ environment, or run the built-in demo dataset.
          </p>
          <button onClick={runDemo} style={{
            width: "100%", padding: "12px", marginBottom: 20, borderRadius: 8,
            background: "var(--color-background-info)", color: "var(--color-text-info)",
            border: "1px solid var(--color-border-info)", fontSize: 14, fontWeight: 500, cursor: "pointer",
          }}>
            Run Demo (synthetic data)
          </button>
          <div style={{ borderTop: "1px solid var(--color-border-tertiary)", paddingTop: 20 }}>
            <form onSubmit={handleUpload}>
              {["queue_managers", "queues", "applications", "channels"].map(name => (
                <div key={name} style={{ marginBottom: 12 }}>
                  <label style={{ display: "block", fontSize: 12, fontWeight: 500, marginBottom: 4, color: "var(--color-text-secondary)", textTransform: "capitalize" }}>
                    {name.replace("_", " ")} CSV
                  </label>
                  <input type="file" name={name} accept=".csv" required style={{ fontSize: 12, width: "100%" }} />
                </div>
              ))}
              <button type="submit" style={{
                marginTop: 8, width: "100%", padding: "10px", borderRadius: 8,
                background: "var(--color-background-success)", color: "var(--color-text-success)",
                border: "1px solid var(--color-border-success)", fontSize: 14, fontWeight: 500, cursor: "pointer",
              }}>
                Analyse My Environment
              </button>
            </form>
          </div>
        </div>
      )}


      {/* ── REVIEW TAB ── */}
      {tab === "review" && result && !loading && (
        <div>
          {result.awaiting_human_review ? (
            <div>
              {/* Header banner */}
              <div style={{ padding: "14px 16px", background: "var(--color-background-warning)", borderRadius: 8, marginBottom: 20, display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 18 }}>⏳</span>
                <div>
                  <p style={{ margin: 0, fontWeight: 600, fontSize: 14, color: "var(--color-text-warning)" }}>Pipeline paused — awaiting your review</p>
                  <p style={{ margin: 0, fontSize: 12, color: "var(--color-text-secondary)", marginTop: 2 }}>Review the proposed target state below. Approve to generate outputs or reject with a reason.</p>
                </div>
              </div>

              {/* Complexity summary */}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 20 }}>
                <div style={{ padding: "14px", background: "var(--color-background-secondary)", borderRadius: 8, textAlign: "center" }}>
                  <div style={{ fontSize: 26, fontWeight: 700, color: "#E24B4A" }}>{result.as_is_metrics?.total_score}</div>
                  <div style={{ fontSize: 11, color: "var(--color-text-tertiary)", marginTop: 4 }}>As-Is Score</div>
                </div>
                <div style={{ padding: "14px", background: "var(--color-background-secondary)", borderRadius: 8, textAlign: "center" }}>
                  <div style={{ fontSize: 26, fontWeight: 700, color: "#1D9E75" }}>{result.target_metrics?.total_score}</div>
                  <div style={{ fontSize: 11, color: "var(--color-text-tertiary)", marginTop: 4 }}>Target Score</div>
                </div>
                <div style={{ padding: "14px", background: "var(--color-background-secondary)", borderRadius: 8, textAlign: "center" }}>
                  <div style={{ fontSize: 26, fontWeight: 700, color: "#1D9E75" }}>{result.complexity_reduction?.reduction_pct}%</div>
                  <div style={{ fontSize: 11, color: "var(--color-text-tertiary)", marginTop: 4 }}>Complexity Reduction</div>
                </div>
              </div>

              {/* ADRs summary */}
              {result.adrs?.length > 0 && (
                <div style={{ marginBottom: 20 }}>
                  <p style={{ fontSize: 13, fontWeight: 500, marginBottom: 8 }}>Architecture Decisions ({result.adrs.length})</p>
                  {result.adrs.map((adr, i) => (
                    <div key={i} style={{ padding: "8px 12px", background: "var(--color-background-secondary)", borderRadius: 6, marginBottom: 6, fontSize: 12 }}>
                      <span style={{ fontWeight: 500, color: "var(--color-text-info)" }}>{adr.id}</span>
                      <span style={{ color: "var(--color-text-secondary)", marginLeft: 8 }}>{adr.decision}</span>
                    </div>
                  ))}
                </div>
              )}

              {/* Topology preview */}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 20 }}>
                <TopologyGraph graphData={result.as_is_graph} title="As-Is Topology" height={240} />
                <TopologyGraph graphData={result.target_graph} title="Proposed Target State" height={240} />
              </div>

              {/* Approve / Reject */}
              <div style={{ border: "1px solid var(--color-border-tertiary)", borderRadius: 8, padding: 16 }}>
                <p style={{ fontSize: 13, fontWeight: 500, marginBottom: 12 }}>Your Decision</p>

                <textarea
                  value={reviewFeedback}
                  onChange={e => setReviewFeedback(e.target.value)}
                  placeholder="If rejecting, describe what needs to change (required for rejection)..."
                  rows={3}
                  style={{ width: "100%", padding: "8px 10px", borderRadius: 6, border: "1px solid var(--color-border-secondary)", fontSize: 12, marginBottom: 12, background: "var(--color-background-primary)", color: "var(--color-text-primary)", resize: "vertical", boxSizing: "border-box" }}
                />

                <div style={{ display: "flex", gap: 10 }}>
                  <button
                    onClick={() => submitReview(true)}
                    disabled={reviewLoading}
                    style={{ flex: 1, padding: "10px", borderRadius: 8, border: "none", background: "#1D9E75", color: "#fff", fontSize: 14, fontWeight: 600, cursor: reviewLoading ? "not-allowed" : "pointer", opacity: reviewLoading ? 0.6 : 1 }}>
                    {reviewLoading ? "Processing..." : "✓ Approve — Generate Outputs"}
                  </button>
                  <button
                    onClick={() => submitReview(false)}
                    disabled={reviewLoading}
                    style={{ flex: 1, padding: "10px", borderRadius: 8, border: "1px solid #E24B4A", background: "transparent", color: "#E24B4A", fontSize: 14, fontWeight: 600, cursor: reviewLoading ? "not-allowed" : "pointer", opacity: reviewLoading ? 0.6 : 1 }}>
                    {reviewLoading ? "Processing..." : "✗ Reject — Redesign with Feedback"}
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <div style={{ padding: "20px", textAlign: "center", color: "var(--color-text-secondary)", fontSize: 13 }}>
              {result.human_approved === true
                ? "✓ You approved this design. Outputs are available in the other tabs."
                : result.human_approved === false
                ? "✗ You rejected this design. The Architect is redesigning — check the Trace tab."
                : "No review pending."}
            </div>
          )}
        </div>
      )}

      {/* ── TOPOLOGY TAB ── */}
      {tab === "topology" && result && !loading && (
        <div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            <TopologyGraph graphData={result.as_is_graph} title="As-Is Topology (current state)" />
            <TopologyGraph graphData={result.target_graph} title="Target State (after transformation)" />
          </div>
          <div style={{ marginTop: 12, padding: "10px 14px", background: "var(--color-background-success)", borderRadius: 8, fontSize: 13 }}>
            <strong style={{ color: "var(--color-text-success)" }}>
              Complexity reduction: {result.complexity_reduction?.reduction_pct}% 
            </strong>
            <span style={{ color: "var(--color-text-secondary)", marginLeft: 8 }}>
              Score: {result.complexity_reduction?.before} → {result.complexity_reduction?.after}
            </span>
            <span style={{ marginLeft: 12, color: result.validation_passed ? "var(--color-text-success)" : "var(--color-text-danger)" }}>
              {result.validation_passed ? "✓ All constraints satisfied" : "✗ Constraint violations found"}
            </span>
          </div>
        </div>
      )}

      {/* ── METRICS TAB ── */}
      {tab === "metrics" && result && !loading && (
        <div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
            <ScoreGauge label="As-Is Complexity Score" score={result.as_is_metrics?.total_score} />
            <ScoreGauge label="Target Complexity Score" score={result.target_metrics?.total_score} />
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 80px 80px 80px", gap: 8, padding: "6px 0", fontSize: 11, color: "var(--color-text-tertiary)", fontWeight: 500 }}>
            <span>Metric</span><span style={{ textAlign: "right" }}>Before</span><span style={{ textAlign: "right" }}>After</span><span style={{ textAlign: "right" }}>Delta</span>
          </div>
          {[
            ["Channel Count (30%)", "channel_count"],
            ["Coupling Index (25%)", "coupling_index"],
            ["Routing Depth (20%)", "routing_depth"],
            ["Fan-Out Score (15%)", "fan_out_score"],
            ["Orphan Objects (10%)", "orphan_objects"],
          ].map(([label, key]) => (
            <MetricRow key={key} label={label}
              before={result.as_is_metrics?.[key] ?? "—"}
              after={result.target_metrics?.[key] ?? "—"} />
          ))}

          {result.constraint_violations?.length > 0 && (
            <div style={{ marginTop: 20 }}>
              <p style={{ fontWeight: 500, fontSize: 13, marginBottom: 8 }}>Constraint Violations</p>
              {result.constraint_violations.map((v, i) => <ViolationBadge key={i} v={v} />)}
            </div>
          )}
        </div>
      )}

      {/* ── ADRs TAB ── */}
      {tab === "adrs" && result && !loading && (
        <div>
          <p style={{ fontSize: 13, color: "var(--color-text-secondary)", marginBottom: 16 }}>
            Architecture Decision Records — every design decision the Architect agent made, with full rationale.
          </p>
          {result.adrs?.length
            ? result.adrs.map((adr, i) => <ADRCard key={i} adr={adr} />)
            : <p style={{ color: "var(--color-text-tertiary)", fontSize: 13 }}>No ADRs recorded.</p>
          }
        </div>
      )}

      {/* ── MQSC TAB ── */}
      {tab === "mqsc" && result && !loading && (
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <p style={{ fontSize: 13, color: "var(--color-text-secondary)", margin: 0 }}>
              Ready-to-run MQSC provisioning commands for the target state.
            </p>
            <button onClick={() => {
              const blob = new Blob([result.mqsc_scripts?.join("\n")], { type: "text/plain" });
              const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
              a.download = "mq_titan_target.mqsc"; a.click();
            }} style={{ fontSize: 12, padding: "6px 12px", borderRadius: 6, border: "1px solid var(--color-border-secondary)", background: "transparent", cursor: "pointer", color: "var(--color-text-primary)" }}>
              Download .mqsc
            </button>
          </div>
          <pre style={{ background: "var(--color-background-secondary)", borderRadius: 8, padding: 16, fontSize: 11, overflowX: "auto", lineHeight: 1.8, color: "var(--color-text-primary)" }}>
            {result.mqsc_scripts?.join("\n") || "No scripts generated."}
          </pre>
        </div>
      )}


      {/* ── CSVS TAB ── */}
      {tab === "csvs" && result && !loading && (
        <div>
          <p style={{ fontSize: 13, color: "var(--color-text-secondary)", marginBottom: 16 }}>
            Target state CSV files — same format as input. Ready to feed into any provisioning tool.
          </p>
          {result.target_csvs && Object.keys(result.target_csvs).length > 0
            ? Object.entries(result.target_csvs).map(([name, content]) => {
              const rows = content.trim().split("\n").length - 1;

                return (
                  <div key={name} style={{ marginBottom: 16, border: "1px solid var(--color-border-tertiary)", borderRadius: 8, overflow: "hidden" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "10px 14px", background: "var(--color-background-secondary)" }}>
                      <div>
                        <span style={{ fontSize: 13, fontWeight: 500 }}>{name}.csv</span>
                        <span style={{ fontSize: 11, color: "var(--color-text-tertiary)", marginLeft: 10 }}>{rows} rows</span>
                      </div>
                      <button onClick={() => {
                        const blob = new Blob([content], { type: "text/csv" });
                        const a = document.createElement("a");
                        a.href = URL.createObjectURL(blob);
                        a.download = name + ".csv";
                        a.click();
                      }} style={{ fontSize: 12, padding: "5px 12px", borderRadius: 6, border: "1px solid var(--color-border-secondary)", background: "transparent", cursor: "pointer", color: "var(--color-text-primary)" }}>
                        Download
                      </button>
                    </div>
                    <pre style={{ margin: 0, padding: "10px 14px", fontSize: 10, overflowX: "auto", background: "var(--color-background-primary)", maxHeight: 160, color: "var(--color-text-secondary)", lineHeight: 1.6 }}>
  {content.trim().split("\n").slice(0, 6).join("\n")}
  {content.trim().split("\n").length > 6 ? "\n... (" + (content.trim().split("\n").length - 6) + " more rows)" : ""}
</pre>
                  </div>
                );
              })
            : <p style={{ color: "var(--color-text-tertiary)", fontSize: 13 }}>No CSV output generated yet.</p>
          }
        </div>
      )}

      {/* ── TRACE TAB ── */}
      {tab === "trace" && result && !loading && (
        <div>
          <p style={{ fontSize: 13, color: "var(--color-text-secondary)", marginBottom: 16 }}>
            Agent execution trace — every agent's findings in order.
          </p>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {result.agent_trace?.map((m, i) => (
              <div key={i} style={{ display: "flex", gap: 12, alignItems: "flex-start", padding: "8px 12px", background: "var(--color-background-secondary)", borderRadius: 6 }}>
                <span style={{ fontSize: 11, fontWeight: 600, minWidth: 100, color: "var(--color-text-info)", paddingTop: 1 }}>{m.agent}</span>
                <span style={{ fontSize: 12, color: "var(--color-text-secondary)", lineHeight: 1.5 }}>{m.msg}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* No result yet on non-upload tabs */}
      {!result && tab !== "upload" && !loading && (
        <p style={{ color: "var(--color-text-tertiary)", fontSize: 13 }}>
          No analysis run yet. Go to the Upload tab and run the demo or upload your CSV files.
        </p>
      )}
    </div>
  );
}

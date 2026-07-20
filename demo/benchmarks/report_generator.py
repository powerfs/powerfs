#!/usr/bin/env python3
import json
import os
import time

def generate_html_report():
    results_dir = "/results"
    
    reports = {}
    for filename in os.listdir(results_dir):
        if filename.endswith("_benchmark.json"):
            benchmark_name = filename.replace("_benchmark.json", "")
            with open(os.path.join(results_dir, filename), "r") as f:
                reports[benchmark_name] = json.load(f)
    
    html_head = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PowerFS Benchmark Report</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); min-height: 100vh; color: #fff; padding: 40px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { text-align: center; font-size: 32px; margin-bottom: 10px; background: linear-gradient(90deg, #00d9ff, #00ff88); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .subtitle { text-align: center; color: #888; margin-bottom: 40px; }
        .benchmark-card { background: rgba(255,255,255,0.05); border-radius: 16px; padding: 24px; margin-bottom: 24px; backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.1); }
        .benchmark-title { font-size: 20px; margin-bottom: 16px; display: flex; align-items: center; gap: 10px; }
        .benchmark-title::before { content: ''; width: 4px; height: 24px; background: linear-gradient(180deg, #00d9ff, #00ff88); border-radius: 2px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1); }
        th { background: rgba(255,255,255,0.05); font-weight: 600; color: #aaa; font-size: 14px; }
        td { font-size: 14px; }
        .ops-value { color: #00ff88; font-weight: 600; }
        .latency-value { color: #00d9ff; }
        .bw-value { color: #ffd700; }
        .config { background: rgba(0,217,255,0.1); border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; font-size: 13px; color: #aaa; }
        .config span { color: #00d9ff; }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .summary-card { background: rgba(255,255,255,0.05); border-radius: 12px; padding: 20px; text-align: center; }
        .summary-label { font-size: 12px; color: #888; margin-bottom: 8px; }
        .summary-value { font-size: 28px; font-weight: 700; }
        .status { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; padding: 4px 12px; border-radius: 20px; background: rgba(0,255,136,0.15); color: #00ff88; margin-top: 16px; }
        .status::before { content: ''; width: 6px; height: 6px; background: #00ff88; border-radius: 50%; }
        .timestamp { text-align: center; color: #666; font-size: 12px; margin-top: 40px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>⚡ PowerFS Benchmark Report</h1>
        <p class="subtitle">Performance Testing Results</p>

        <div class="summary-grid">
            <div class="summary-card">
                <div class="summary-label">Benchmarks</div>
                <div class="summary-value" style="color: #00d9ff;">""" + str(len(reports)) + """</div>
            </div>
            <div class="summary-card">
                <div class="summary-label">Operations</div>
                <div class="summary-value" style="color: #00ff88;">""" + str(sum(len(r.get('operations', [])) for r in reports.values())) + """</div>
            </div>
            <div class="summary-card">
                <div class="summary-label">Status</div>
                <div class="summary-value" style="color: #ffd700;">Completed</div>
            </div>
        </div>"""
    
    html_body = ""
    for name, report in reports.items():
        is_ops = name == 'kv' or name == 'metadata'
        bw_label = 'AVG OPS/S' if is_ops else 'BW (MB/s)'
        value_class = 'ops-value' if is_ops else 'bw-value'
        
        config_str = json.dumps(report.get('config', {}), indent=2, ensure_ascii=False).replace('"', "'").replace('\n', '<br>').replace('    ', '&nbsp;&nbsp;&nbsp;&nbsp;')
        
        html_body += """
        <div class="benchmark-card">
            <div class="benchmark-title>""" + name.upper() + """ Benchmark</div>
            <div class="config">
                <span>Config:</span> """ + config_str + """
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Operation</th>
                        <th>Rounds</th>
                        <th>""" + bw_label + """</th>
                        <th>Avg Latency (ms)</th>
                    </tr>
                </thead>
                <tbody>"""
        
        summary = report.get('summary', {})
        for op, data in summary.items():
            rounds = report.get('config', {}).get('rounds', 1)
            val = data.get('avg_ops_per_sec', data.get('avg_bandwidth_mbps', 0))
            lat = data.get('avg_latency_ms', 0)
            html_body += """
                    <tr>
                        <td>""" + op + """</td>
                        <td>""" + str(rounds) + """</td>
                        <td class="""" + value_class + """">""" + f"{val:.2f}" + """</td>
                        <td class="latency-value">""" + f"{lat:.4f}" + """</td>
                    </tr>"""
        
        html_body += """
                </tbody>
            </table>
            <div class="status">✓ Benchmark completed successfully</div>
        </div>"""
    
    html_footer = """
        <div class="timestamp">Generated at """ + time.strftime('%Y-%m-%d %H:%M:%S') + """</div>
    </div>
</body>
</html>"""
    
    html = html_head + html_body + html_footer
    
    with open("/results/report.html", "w") as f:
        f.write(html)
    
    print("📄 HTML report generated: /results/report.html")

if __name__ == "__main__":
    generate_html_report()
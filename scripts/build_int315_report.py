#!/usr/bin/env python3
"""
CityPulse — Comprehensive INT315 Cluster Computing Project Report Generator
Substitutes real project source code and screenshots into report/template.html
and compiles the PDF report using headless Google Chrome.
"""

import os
import sys
import base64
import subprocess

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DIR = os.path.join(ROOT_DIR, "report")
TEMPLATE_PATH = os.path.join(REPORT_DIR, "template.html")
IMAGES_DIR = os.path.join(REPORT_DIR, "images")
OUTPUT_HTML = os.path.join(REPORT_DIR, "INT315_CityPulse_Project_Report.html")
OUTPUT_PDF = os.path.join(REPORT_DIR, "INT315_CityPulse_Project_Report.pdf")

def read_file(rel_path):
    full_path = os.path.join(ROOT_DIR, rel_path)
    if os.path.exists(full_path):
        with open(full_path, "r", encoding="utf-8") as f:
            return f.read()
    return f"// File not found: {rel_path}"

def get_base64_image(filename):
    full_path = os.path.join(IMAGES_DIR, filename)
    if os.path.exists(full_path):
        with open(full_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
            return f"data:image/png;base64,{encoded}"
    print(f"Warning: image {filename} not found in {IMAGES_DIR}")
    return ""

def html_escape(text):
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )

def main():
    print("Reading template...")
    if not os.path.exists(TEMPLATE_PATH):
        print(f"Error: Template not found at {TEMPLATE_PATH}")
        sys.exit(1)

    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    print("Loading source code...")
    replacements = {
        "__CODE_SPARK_STREAMING__": html_escape(read_file("spark_jobs/kafka_stream.py")),
        "__CODE_SCALA_JOB__": html_escape(read_file("spark_jobs/CityPulseAnalytics.scala")),
        "__CODE_CONSUMER__": html_escape(read_file("ingestion/consumer.py")),
        "__CODE_ANOMALY__": html_escape(read_file("ingestion/anomaly_detector.py")),
        "__CODE_SPARK_SQL__": html_escape(read_file("dashboard/app_pages/spark_sql.py")),
        "__CODE_GRAPHX__": html_escape(read_file("dashboard/app_pages/graph_analytics.py")),
        "__CODE_MLLIB__": html_escape(read_file("dashboard/app_pages/ml_lab.py")),
    }

    print("Encoding screenshots...")
    image_keys = {
        "__IMG_LPU_LOGO__": "lpu_logo.png",
        "__IMG_MAIN__": "main_dashboard.png",
        "__IMG_MAIN_TABLE__": "main_dashboard_table.png",
        "__IMG_LIVE__": "live_monitoring.png",
        "__IMG_MAP__": "pollution_map.png",
        "__IMG_HIST__": "historical_analytics.png",
        "__IMG_ALERTS__": "alerts_centre.png",
        "__IMG_ANOMALY__": "anomaly_detection.png",
        "__IMG_SPARK_SQL__": "spark_sql.png",
        "__IMG_MLLIB__": "spark_mllib.png",
        "__IMG_GRAPH__": "graph_analytics.png",
        "__IMG_KAFKA__": "kafka_pipeline.png",
        "__IMG_RDD__": "rdd_lab.png",
        "__IMG_SCALA__": "scala_job.png",
        "__IMG_HEALTH__": "system_health.png",
    }

    for placeholder, img_filename in image_keys.items():
        replacements[placeholder] = get_base64_image(img_filename)

    print("Performing template substitutions...")
    for key, val in replacements.items():
        html = html.replace(key, val)

    print(f"Writing compiled HTML report to: {OUTPUT_HTML}")
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    html_size_mb = os.path.getsize(OUTPUT_HTML) / (1024 * 1024)
    print(f"HTML Report generated ({html_size_mb:.2f} MB).")

    print("Compiling PDF with Google Chrome Headless...")
    chrome_bin = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    chrome_cmd = [
        chrome_bin,
        "--headless",
        "--disable-gpu",
        "--run-all-compositor-stages-before-draw",
        "--no-pdf-header-footer",
        f"--print-to-pdf={OUTPUT_PDF}",
        OUTPUT_HTML,
    ]

    res = subprocess.run(chrome_cmd, capture_output=True, text=True)
    if os.path.exists(OUTPUT_PDF):
        pdf_size_mb = os.path.getsize(OUTPUT_PDF) / (1024 * 1024)
        print(f"============================================================")
        print(f"SUCCESS: Report PDF generated successfully!")
        print(f"Path: {OUTPUT_PDF}")
        print(f"Size: {pdf_size_mb:.2f} MB")
        print(f"============================================================")
    else:
        print(f"Compilation error. Chrome output:\n{res.stderr}")
        sys.exit(1)

if __name__ == "__main__":
    main()

import os
import csv

BASE_DIR = "shared_metrics"
OUTPUT_DIR = "csv_exports"

def get_all_metrics(filepath):
    metrics = {}
    if not os.path.exists(filepath):
        return metrics
    
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                
                parts = line.split()
                if len(parts) >= 2:
                    raw_metric_name = parts[0]
                    clean_metric_name = raw_metric_name.split('{')[0]
                    
                    try:
                        metric_value = float(parts[-1])
                        metrics[clean_metric_name] = metric_value
                    except ValueError:
                        metrics[clean_metric_name] = parts[-1]
    except Exception as e:
        print(f"⚠️ Error reading {filepath}: {e}")
        
    return metrics

def print_and_export_table(title, filename, rows):
    if not rows:
        print(f"⚠️ No data found for {title}. Check if your .prom files exist.")
        return

    fieldnames = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    
    base_cols = ["Broker", "Consumers", "Producers", "Message Size (bytes)", "Delivery Mode", "Target"]
    ordered_fields = [col for col in base_cols if col in fieldnames]
    ordered_fields += [col for col in fieldnames if col not in ordered_fields]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, filename)
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=ordered_fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n📊 **{title}**")
    print(f"📁 Full data saved to: `{csv_path}`")
    
    display_fields = ordered_fields[:5] 
    if len(ordered_fields) > 5:
        display_fields.append("... (+ more metrics)")

    header_str = " | ".join([str(h) for h in display_fields])
    print(f"| {header_str} |")
    print("|" + "|".join(["---" for _ in display_fields]) + "|")

    for row in rows:
        row_values = []
        for col in display_fields:
            if col == "... (+ more metrics)":
                row_values.append("...")
            else:
                row_values.append(str(row.get(col, "N/A")))
        print(f"| {' | '.join(row_values)} |")


def extract_scenario_1():
    brokers = ["rabbitmq", "kafka"]
    sizes = [100, 1024, 10240, 51200, 102400]
    rows = []
    
    for broker in brokers:
        for size in sizes:
            filepath = f"{BASE_DIR}/sc1/{broker}_size{size}_run1.prom"
            metrics = get_all_metrics(filepath)
            if metrics:
                row_data = {"Broker": broker, "Message Size (bytes)": size}
                row_data.update(metrics)
                rows.append(row_data)
            
    print_and_export_table("Scenario 1: Message Size Impact", "sc1_message_size.csv", rows)

def extract_scenario_2():
    brokers = ["rabbitmq", "kafka"]
    clients = [1, 2, 4]
    rows = []
    
    for broker in brokers:
        for count in clients:
            filepath = f"{BASE_DIR}/sc2/{broker}_producer{count}_consumer{count}_run1.prom"
            metrics = get_all_metrics(filepath)
            if metrics:
                row_data = {"Broker": broker, "Producers": count, "Consumers": count}
                row_data.update(metrics)
                rows.append(row_data)
            
    print_and_export_table("Scenario 2: Parallelism Impact", "sc2_parallelism.csv", rows)

def extract_scenario_3():
    brokers = ["rabbitmq", "kafka"]
    modes = ["persistent", "transient"]
    client_count = 4
    rows = []
    
    for broker in brokers:
        for mode in modes:
            filepath = f"{BASE_DIR}/sc3/{broker}_producer{client_count}_consumer{client_count}_delivery-{mode}_run1.prom"
            metrics = get_all_metrics(filepath)
            if metrics:
                row_data = {"Broker": broker, "Delivery Mode": mode, "Producers": client_count, "Consumers": client_count}
                row_data.update(metrics)
                rows.append(row_data)
            
    print_and_export_table("Scenario 3: Persistent vs Transient", "sc3_delivery_modes.csv", rows)

def extract_scenario_4():
    brokers = ["rabbitmq", "kafka"]
    clients = [1, 4, 16]
    rows = []
    
    for broker in brokers:
        for count in clients:
            # Producers
            prod_path = f"{BASE_DIR}/sc4/{broker}_producer_prepopulate_run{count}.prom"
            prod_metrics = get_all_metrics(prod_path)
            if prod_metrics:
                prod_row = {"Broker": broker, "Target": "Producer", "Producers": 4, "Consumers": count}
                prod_row.update(prod_metrics)
                rows.append(prod_row)

            # Consumers
            cons_path = f"{BASE_DIR}/sc4/{broker}_consumer-only{count}.prom"
            cons_metrics = get_all_metrics(cons_path)
            if cons_metrics:
                cons_row = {"Broker": broker, "Target": "Consumer", "Producers": 0, "Consumers": count}
                cons_row.update(cons_metrics)
                rows.append(cons_row)
            
    print_and_export_table("Scenario 4: Producer vs Consumer Isolation", "sc4_isolation.csv", rows)

if __name__ == "__main__":
    print("🚀 Firing up massive extraction process...\n")
    extract_scenario_1()
    extract_scenario_2()
    extract_scenario_3()
    extract_scenario_4()
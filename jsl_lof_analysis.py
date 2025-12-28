import requests
import pandas as pd
import time
import os
from datetime import datetime
import json

# --- 配置 ---
OUTPUT_DIR = "jsl_data"
timestamp = datetime.now().strftime('%Y%m%d')
MAIN_OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"jsl_lof_analysis_{timestamp}.xlsx")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://www.jisilu.cn/data/lof/lof_list/',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
}

def clean_and_filter(df, source_name):
    """清洗 volume 和 discount_rt 字段，并筛选符合条件的数据"""
    df = df.copy()
    
    # 清洗 volume: 去逗号，转数字
    if 'volume' in df.columns:
        vol_str = df['volume'].astype(str).str.replace(',', '').str.strip()
        df['volume_clean'] = pd.to_numeric(vol_str, errors='coerce')
    else:
        df['volume_clean'] = pd.NA

    # 清洗 discount_rt: 去 '%'，转数字
    if 'discount_rt' in df.columns:
        disc_str = df['discount_rt'].astype(str).str.rstrip('%').str.strip()
        df['discount_rt_clean'] = pd.to_numeric(disc_str, errors='coerce')
    else:
        df['discount_rt_clean'] = pd.NA

    # 筛选条件
    filtered = df[
        (df['volume_clean'] > 1000) &
        (df['discount_rt_clean'] > 9)
    ].copy()

    return df, filtered


def safe_sheet_name(name):
    for char in ['\\', '/', '?', '*', '[', ']', ':']:
        name = name.replace(char, '_')
    return name[:31]


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = requests.Session()
    session.headers.update(HEADERS)

    all_sheets = {}          # 所有要写入 Excel 的 sheet
    all_filtered_dfs = []    # 收集所有筛选后的 DataFrame，用于汇总

    # ==================== 1. 股票LOF ====================
    print("🔍 获取股票LOF数据...")
    try:
        url = "https://www.jisilu.cn/data/lof/stock_lof_list/"
        payload = {'___jsl': json.dumps({"page":1,"rp":50,"sortname":"溢价率","sortorder":"asc","query":""})}
        resp = session.post(url, data=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        if 'rows' in data and data['rows']:
            raw_df = pd.DataFrame([row.get('cell', row) for row in data['rows']])
            full, filtered = clean_and_filter(raw_df, "股票LOF")
            all_sheets['股票LOF_全量'] = full
            if not filtered.empty:
                all_sheets['股票LOF_筛选'] = filtered
                all_filtered_dfs.append(filtered.assign(source='股票LOF'))
                print(f"✅ 股票LOF：{len(filtered)} 条满足条件")
            else:
                print("ℹ️ 股票LOF：无满足条件数据")
        else:
            print("⚠️ 股票LOF：无数据返回")
    except Exception as e:
        print(f"❌ 股票LOF失败: {e}")

    # ==================== 2. 指数LOF ====================
    print("🔍 获取指数LOF数据...")
    try:
        url = "https://www.jisilu.cn/data/lof/index_lof_list/"
        payload = {'___jsl': json.dumps({"page":1,"rp":50,"sortname":"溢价率","sortorder":"asc","query":""})}
        resp = session.post(url, data=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        if 'rows' in data and data['rows']:
            raw_df = pd.DataFrame([row.get('cell', row) for row in data['rows']])
            full, filtered = clean_and_filter(raw_df, "指数LOF")
            all_sheets['指数LOF_全量'] = full
            if not filtered.empty:
                all_sheets['指数LOF_筛选'] = filtered
                all_filtered_dfs.append(filtered.assign(source='指数LOF'))
                print(f"✅ 指数LOF：{len(filtered)} 条满足条件")
            else:
                print("ℹ️ 指数LOF：无满足条件数据")
        else:
            print("⚠️ 指数LOF：无数据返回")
    except Exception as e:
        print(f"❌ 指数LOF失败: {e}")

    # ==================== 3. QDII 欧美 ====================
    print("🔍 获取QDII欧美市场LOF数据...")
    try:
        url = "https://www.jisilu.cn/data/qdii/qdii_list/E"
        params = {'___jsl': f'LST___t={int(time.time()*1000)}', 'only_lof': 'y', 'rp': 50}
        resp = session.get(url, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        if 'rows' in data and data['rows']:
            raw_df = pd.DataFrame([row.get('cell', row) for row in data['rows']])
            full, filtered = clean_and_filter(raw_df, "QDII_欧美")
            all_sheets['QDII_欧美_全量'] = full
            if not filtered.empty:
                all_sheets['QDII_欧美_筛选'] = filtered
                all_filtered_dfs.append(filtered.assign(source='QDII_欧美'))
                print(f"✅ QDII欧美：{len(filtered)} 条满足条件")
            else:
                print("ℹ️ QDII欧美：无满足条件数据")
        else:
            print("⚠️ QDII欧美：无数据返回")
    except Exception as e:
        print(f"❌ QDII欧美失败: {e}")

    # ==================== 4. QDII 亚洲 ====================
    print("🔍 获取QDII亚洲市场LOF数据...")
    try:
        url = "https://www.jisilu.cn/data/qdii/qdii_list/A"
        params = {'___jsl': f'LST___t={int(time.time()*1000)}', 'only_lof': 'y', 'rp': 50}
        resp = session.get(url, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        if 'rows' in data and data['rows']:
            raw_df = pd.DataFrame([row.get('cell', row) for row in data['rows']])
            full, filtered = clean_and_filter(raw_df, "QDII_亚洲")
            all_sheets['QDII_亚洲_全量'] = full
            if not filtered.empty:
                all_sheets['QDII_亚洲_筛选'] = filtered
                all_filtered_dfs.append(filtered.assign(source='QDII_亚洲'))
                print(f"✅ QDII亚洲：{len(filtered)} 条满足条件")
            else:
                print("ℹ️ QDII亚洲：无满足条件数据")
        else:
            print("⚠️ QDII亚洲：无数据返回")
    except Exception as e:
        print(f"❌ QDII亚洲失败: {e}")

    # ==================== 5. QDII 商品 ====================
    print("🔍 获取QDII商品市场LOF数据...")
    try:
        url = "https://www.jisilu.cn/data/qdii/qdii_list/C"
        params = {'___jsl': f'LST___t={int(time.time()*1000)}', 'only_lof': 'y', 'rp': 50}
        resp = session.get(url, params=params, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        if 'rows' in data and data['rows']:
            raw_df = pd.DataFrame([row.get('cell', row) for row in data['rows']])
            full, filtered = clean_and_filter(raw_df, "QDII_商品")
            all_sheets['QDII_商品_全量'] = full
            if not filtered.empty:
                all_sheets['QDII_商品_筛选'] = filtered
                all_filtered_dfs.append(filtered.assign(source='QDII_商品'))
                print(f"✅ QDII商品：{len(filtered)} 条满足条件")
            else:
                print("ℹ️ QDII商品：无满足条件数据")
        else:
            print("⚠️ QDII商品：无数据返回")
    except Exception as e:
        print(f"❌ QDII商品失败: {e}")

    # ==================== 汇总所有筛选结果到一个 sheet ====================
    if all_filtered_dfs:
        combined_filtered = pd.concat(all_filtered_dfs, ignore_index=True)
        all_sheets['全部筛选结果'] = combined_filtered

        # 控制台打印
        display_cols = ['fund_nm', 'fund_id', 'volume', 'discount_rt', 'source']
        available = [c for c in display_cols if c in combined_filtered.columns]
        display_df = combined_filtered[available]

        print("\n" + "="*80)
        print("🎯 符合条件的基金（成交量 > 1000万 且 溢价率 > 9%）:")
        print("="*80)
        for _, row in display_df.iterrows():
            name = row.get('fund_nm', row.get('fund_id', 'N/A'))
            vol = row.get('volume', 'N/A')
            prem = row.get('discount_rt', 'N/A')
            src = row['source']
            print(f"【{src}】{name} | 成交量: {vol} | 溢价率: {prem}%")
        print("="*80)
        print(f"📌 共 {len(combined_filtered)} 只基金满足条件。")
    else:
        print("\n❌ 无任何基金满足筛选条件（成交量>1000万 且 溢价率>9%）。")

    # ==================== 保存到单一 Excel 文件 ====================
    if all_sheets:
        with pd.ExcelWriter(MAIN_OUTPUT_FILE, engine='openpyxl') as writer:
            for sheet_name, df in all_sheets.items():
                safe_name = safe_sheet_name(sheet_name)
                df.to_excel(writer, sheet_name=safe_name, index=False)
        print(f"\n📁 所有数据已保存至单一文件：\n{MAIN_OUTPUT_FILE}")
    else:
        print("\n❌ 未获取到任何有效数据，未生成文件。")

    print("\n✨ 脚本执行完毕。")


if __name__ == "__main__":
    main()
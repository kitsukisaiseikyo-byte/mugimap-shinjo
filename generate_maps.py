"""
麦生育マップ - GitHub Actions自動更新版
新規Sentinel-2画像を検出して自動でマップ更新
"""

import ee
import pandas as pd
import folium
from folium import FeatureGroup
import numpy as np
import os
import datetime as dt
import json
import argparse

# ===== 引数パース =====
parser = argparse.ArgumentParser()
parser.add_argument('--last-date', type=str, default='2024-12-01', help='前回処理日')
args = parser.parse_args()

# ===== Earth Engine初期化（サービスアカウント） =====
try:
    credentials = ee.ServiceAccountCredentials(
        email=os.environ.get('GEE_SERVICE_ACCOUNT'),
        key_file='private-key.json'
    )
    ee.Initialize(credentials, project='ee-kitsukisaiseikyo')
except Exception as e:
    print(f"GEE初期化エラー: {e}")
    exit(1)

print("="*70)
print("麦生育マップ - 自動更新版")
print("="*70)

# ===== 設定 =====
FIELD_ASSET = 'projects/ee-kitsukisaiseikyo/assets/2025442101'
TARGET_FIELDS_PATH = '新庄麦筆リスト.xlsx'
OUTPUT_DIR = 'output'
STATE_FILE = 'last_processed.txt'

START_DATE = '2024-12-01'
END_DATE = dt.datetime.now().strftime('%Y-%m-%d')
PIXEL_SCALE = 10
CLOUD_THRESHOLD = 20  # 雲量20%以下

os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"\n前回処理日: {args.last_date}")
print(f"検索期間: {args.last_date} 〜 {END_DATE}")
print(f"雲量閾値: {CLOUD_THRESHOLD}%以下")

# ===== データ読み込み =====
print("\n[1] データ読み込み中...")
target_fields_df = pd.read_excel(TARGET_FIELDS_PATH)
print(f"  ✓ 対象筆数: {len(target_fields_df)}筆")

field_polygons = ee.FeatureCollection(FIELD_ASSET)
target_polygon_ids = target_fields_df['polygon_uu'].tolist()
target_polygons = field_polygons.filter(ee.Filter.inList('polygon_uu', target_polygon_ids))

# ===== Sentinel-2取得（新規画像のみ） =====
print("\n[2] 新規Sentinel-2画像検索中...")

def mask_s2_clouds(image):
    qa = image.select('QA60')
    mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
    return image.updateMask(mask).divide(10000)

def add_indices(image):
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    evi = image.expression(
        '2.5*((NIR-RED)/(NIR+6*RED-7.5*BLUE+1))',
        {'NIR': image.select('B8'), 'RED': image.select('B4'), 'BLUE': image.select('B2')}
    ).rename('EVI')
    lai = ndvi.multiply(-1).add(1).log().divide(-0.5).rename('LAI')
    return image.addBands([ndvi, evi, lai])

# 前回処理日以降の新規画像のみ取得
s2_collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
    .filterBounds(target_polygons.geometry())
    .filterDate(args.last_date, END_DATE)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', CLOUD_THRESHOLD))
    .map(mask_s2_clouds)
    .map(add_indices)
)

new_image_count = s2_collection.size().getInfo()
print(f"  ✓ 新規画像数: {new_image_count}枚")

if new_image_count == 0:
    print("\n⚠️ 新規画像なし。処理をスキップします。")
    exit(0)

# ===== 既存データ読み込み =====
print("\n[3] 既存データ読み込み中...")

history_file = os.path.join(OUTPUT_DIR, 'observation_history.json')
if os.path.exists(history_file):
    with open(history_file, 'r', encoding='utf-8') as f:
        history = json.load(f)
    print(f"  ✓ 既存観測日数: {len(history['dates'])}日")
else:
    history = {
        'dates': [],
        'date_to_index': {},
        'pixel_counts': {}
    }
    print("  ✓ 新規作成")

# ===== 新規観測日取得 =====
print("\n[4] 新規観測日取得中...")

collection_info = s2_collection.getInfo()
new_dates = []

for feature in collection_info.get('features', []):
    props = feature.get('properties', {})
    if 'system:index' not in props:
        continue
    idx = props['system:index']
    date_obj = dt.datetime.strptime(idx[:8], '%Y%m%d')
    date_str = date_obj.strftime('%Y-%m-%d')
    
    if date_str not in history['date_to_index']:
        history['date_to_index'][date_str] = idx
        new_dates.append(date_str)

new_dates = sorted(new_dates)
print(f"  ✓ 新規観測日数: {len(new_dates)}日")

if len(new_dates) == 0:
    print("\n⚠️ 処理対象の新規日付なし。")
    exit(0)

for i, date in enumerate(new_dates, 1):
    print(f"    {i}. {date}")

# ===== マップ中心座標 =====
print("\n[5] 筆ポリゴン情報取得中...")
fields_info = target_polygons.getInfo()
coords = target_polygons.geometry().bounds().getInfo()['coordinates'][0]
center_lon = sum([c[0] for c in coords]) / len(coords)
center_lat = sum([c[1] for c in coords]) / len(coords)
print(f"  ✓ マップ中心: ({center_lat:.4f}, {center_lon:.4f})")

# ===== カラーマップ関数 =====
def get_lai_color(lai):
    if lai is None or np.isnan(lai):
        return '#808080'
    if lai < 0.5:
        return '#d73027'
    if lai < 1.0:
        return '#fc8d59'
    if lai < 2.0:
        return '#fee08b'
    if lai < 3.0:
        return '#91cf60'
    return '#1a9850'

def get_ndvi_color(ndvi):
    if ndvi is None or np.isnan(ndvi):
        return '#808080'
    if ndvi < 0.2:
        return '#d73027'
    if ndvi < 0.4:
        return '#fc8d59'
    if ndvi < 0.6:
        return '#fee08b'
    if ndvi < 0.8:
        return '#91cf60'
    return '#1a9850'

# ===== 既存マップ読み込みまたは新規作成 =====
print("\n[6] マップ初期化中...")

m_lai = folium.Map(location=[center_lat, center_lon], zoom_start=15, tiles='OpenStreetMap')
m_ndvi = folium.Map(location=[center_lat, center_lon], zoom_start=15, tiles='OpenStreetMap')

# ===== 新規日付のレイヤー追加 =====
print("\n[7] 新規日付処理中...")

for date_idx, date in enumerate(new_dates):
    print(f"\n  === [{date_idx+1}/{len(new_dates)}] {date} 処理中 ===")
    
    target_index = history['date_to_index'][date]
    target_image = s2_collection.filter(ee.Filter.eq('system:index', target_index)).first()
    
    # LAIレイヤー
    layer_lai = FeatureGroup(name=f'LAI_{date}', show=(date == new_dates[-1]))
    layer_ndvi = FeatureGroup(name=f'NDVI_{date}', show=(date == new_dates[-1]))
    
    date_pixels = 0
    
    for field_idx, feature in enumerate(fields_info['features']):
        if feature['geometry']['type'] != 'Polygon':
            continue
        
        polygon_uu = feature['properties'].get('polygon_uu')
        address = target_fields_df[target_fields_df['polygon_uu'] == polygon_uu]['address'].values
        address = address[0] if len(address) > 0 else '不明'
        
        print(f"    [{field_idx+1}/{len(fields_info['features'])}] {address}...", end='', flush=True)
        
        field_geom = ee.Geometry.Polygon(feature['geometry']['coordinates'])
        
        try:
            # LAIサンプリング
            sample_lai = target_image.select(['LAI']).sample(
                region=field_geom,
                scale=PIXEL_SCALE,
                geometries=True
            ).getInfo()
            
            # NDVIサンプリング
            sample_ndvi = target_image.select(['NDVI']).sample(
                region=field_geom,
                scale=PIXEL_SCALE,
                geometries=True
            ).getInfo()
            
            if 'features' not in sample_lai or 'features' not in sample_ndvi:
                print(" データなし")
                continue
            
            pixel_count = len(sample_lai['features'])
            
            # LAIピクセル追加
            for pixel_feature in sample_lai['features']:
                geom = pixel_feature.get('geometry', {})
                props = pixel_feature.get('properties', {})
                if not geom or not props:
                    continue
                
                lon, lat = geom['coordinates']
                lai = props.get('LAI')
                
                half_size = PIXEL_SCALE / 2 / 111320
                bounds = [[lat - half_size, lon - half_size], [lat + half_size, lon + half_size]]
                
                lai_str = f"{lai:.2f}" if lai is not None and not np.isnan(lai) else 'N/A'
                
                folium.Rectangle(
                    bounds=bounds,
                    color=get_lai_color(lai),
                    fill=True,
                    fillColor=get_lai_color(lai),
                    fillOpacity=0.8,
                    weight=0.5,
                    popup=f"<b>{address}</b><br>日付: {date}<br>LAI: {lai_str}",
                    tooltip=f"{date}: LAI {lai_str}"
                ).add_to(layer_lai)
            
            # NDVIピクセル追加
            for pixel_feature in sample_ndvi['features']:
                geom = pixel_feature.get('geometry', {})
                props = pixel_feature.get('properties', {})
                if not geom or not props:
                    continue
                
                lon, lat = geom['coordinates']
                ndvi = props.get('NDVI')
                
                half_size = PIXEL_SCALE / 2 / 111320
                bounds = [[lat - half_size, lon - half_size], [lat + half_size, lon + half_size]]
                
                ndvi_str = f"{ndvi:.3f}" if ndvi is not None and not np.isnan(ndvi) else 'N/A'
                
                folium.Rectangle(
                    bounds=bounds,
                    color=get_ndvi_color(ndvi),
                    fill=True,
                    fillColor=get_ndvi_color(ndvi),
                    fillOpacity=0.8,
                    weight=0.5,
                    popup=f"<b>{address}</b><br>日付: {date}<br>NDVI: {ndvi_str}",
                    tooltip=f"{date}: NDVI {ndvi_str}"
                ).add_to(layer_ndvi)
            
            date_pixels += pixel_count
            print(f" {pixel_count}px")
            
        except Exception as e:
            print(f" エラー: {e}")
            continue
    
    # 筆境界線追加
    for feature in fields_info['features']:
        if feature['geometry']['type'] == 'Polygon':
            coords_poly = [[lat, lon] for lon, lat in feature['geometry']['coordinates'][0]]
            folium.Polygon(coords_poly, color='#000000', weight=2, fill=False).add_to(layer_lai)
            folium.Polygon(coords_poly, color='#000000', weight=2, fill=False).add_to(layer_ndvi)
    
    layer_lai.add_to(m_lai)
    layer_ndvi.add_to(m_ndvi)
    
    history['dates'].append(date)
    history['pixel_counts'][date] = date_pixels
    
    print(f"  ✓ {date}: {date_pixels}ピクセル")

# ===== LayerControl追加 =====
folium.LayerControl(position='topright', collapsed=False).add_to(m_lai)
folium.LayerControl(position='topright', collapsed=False).add_to(m_ndvi)

# ===== レイヤー操作ボタン =====
layer_control_script = '''
<div id="layerButtons" style="
    position: fixed;
    right: 10px;
    z-index: 1000;
    background: white;
    padding: 5px;
    border-radius: 8px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.3);
    font-size: 14px;
">
  <button onclick="selectAllLayers()" style="margin:2px;">全選択</button>
  <button onclick="deselectAllLayers()" style="margin:2px;">全解除</button>
</div>

<script>
function selectAllLayers() {
  document.querySelectorAll('.leaflet-control-layers-selector').forEach(cb => {
    if (!cb.checked) cb.click();
  });
}
function deselectAllLayers() {
  document.querySelectorAll('.leaflet-control-layers-selector').forEach(cb => {
    if (cb.checked) cb.click();
  });
}

function adjustButtonPosition() {
  const ctrl = document.querySelector('.leaflet-control-layers');
  const btns = document.getElementById('layerButtons');
  if (ctrl && btns) {
    const rect = ctrl.getBoundingClientRect();
    btns.style.top = (rect.bottom + 8) + 'px';
  }
}
setInterval(adjustButtonPosition, 500);
</script>
'''
m_lai.get_root().html.add_child(folium.Element(layer_control_script))
m_ndvi.get_root().html.add_child(folium.Element(layer_control_script))

# ===== タイトル =====
all_dates = sorted(history['dates'])
total_pixels = sum(history['pixel_counts'].values())

title_lai = f'''
<div style="position: fixed; top: 10px; left: 50px; width: 600px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border: 3px solid white; z-index: 9999; padding: 15px;
            border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.3); color: white;">
    <h3 style="margin: 0; font-size: 20px;">🌾 LAI ピクセルマップ（自動更新版）</h3>
    <p style="margin: 5px 0 0 0; font-size: 13px; opacity: 0.9;">
        📅 {all_dates[0]} 〜 {all_dates[-1]} ({len(all_dates)}日観測)<br>
        📍 {len(fields_info['features'])}筆 | 🔲 総ピクセル数: {total_pixels:,}<br>
        🆕 最新更新: {new_dates[-1]} | ☁️ 雲量{CLOUD_THRESHOLD}%以下<br>
        右上のレイヤーで日付を選択
    </p>
</div>
'''
m_lai.get_root().html.add_child(folium.Element(title_lai))

title_ndvi = f'''
<div style="position: fixed; top: 10px; left: 50px; width: 600px;
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            border: 3px solid white; z-index: 9999; padding: 15px;
            border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.3); color: white;">
    <h3 style="margin: 0; font-size: 20px;">🌾 NDVI ピクセルマップ（自動更新版）</h3>
    <p style="margin: 5px 0 0 0; font-size: 13px; opacity: 0.9;">
        📅 {all_dates[0]} 〜 {all_dates[-1]} ({len(all_dates)}日観測)<br>
        📍 {len(fields_info['features'])}筆<br>
        🆕 最新更新: {new_dates[-1]} | ☁️ 雲量{CLOUD_THRESHOLD}%以下<br>
        右上のレイヤーで日付を選択
    </p>
</div>
'''
m_ndvi.get_root().html.add_child(folium.Element(title_ndvi))

# ===== 凡例 =====
legend_html = '''
<div style="position: fixed; bottom: 50px; right: 50px; width: 200px;
            background-color: white; border: 3px solid #2c3e50; z-index: 9999;
            padding: 15px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.3);">
<h4 style="margin:0 0 10px 0; border-bottom:2px solid #3498db; padding-bottom:5px;">LAI / NDVI</h4>
<p style="margin:5px 0;"><span style="color:#d73027; font-size:20px;">■</span> 低</p>
<p style="margin:5px 0;"><span style="color:#fc8d59; font-size:20px;">■</span> やや低</p>
<p style="margin:5px 0;"><span style="color:#fee08b; font-size:20px;">■</span> 中</p>
<p style="margin:5px 0;"><span style="color:#91cf60; font-size:20px;">■</span> 高</p>
<p style="margin:5px 0;"><span style="color:#1a9850; font-size:20px;">■</span> 非常に高</p>
</div>
'''
m_lai.get_root().html.add_child(folium.Element(legend_html))
m_ndvi.get_root().html.add_child(folium.Element(legend_html))

# ===== 保存 =====
print("\n[8] マップ保存中...")

map_lai_path = os.path.join(OUTPUT_DIR, 'index.html')  # GitHub Pagesのトップページ
map_ndvi_path = os.path.join(OUTPUT_DIR, 'ndvi.html')

m_lai.save(map_lai_path)
m_ndvi.save(map_ndvi_path)

print(f"  ✓ LAIマップ: {map_lai_path}")
print(f"  ✓ NDVIマップ: {map_ndvi_path}")

# ===== 履歴保存 =====
with open(history_file, 'w', encoding='utf-8') as f:
    json.dump(history, f, ensure_ascii=False, indent=2)
print(f"  ✓ 履歴保存: {history_file}")

# ===== 最終処理日更新 =====
with open(STATE_FILE, 'w') as f:
    f.write(new_dates[-1])
print(f"  ✓ 最終処理日: {new_dates[-1]}")

print("\n" + "="*70)
print("✓ 自動更新完了！")
print("="*70)
print(f"\n新規追加: {len(new_dates)}日")
print(f"総観測日数: {len(history['dates'])}日")
print(f"総ピクセル数: {total_pixels:,}")
print("\n追加された日付:")
for date in new_dates:
    print(f"  - {date} ({history['pixel_counts'][date]:,}ピクセル)")
print("\n" + "="*70)

"""
麦生育マップ - GitHub Actions自動更新版 (キャッシュ機構搭載)
NDVI、NDWI、GNDVI の3つのマップを作成
各日付のデータをGeoJSONキャッシュとして保存し、新規日付のみ処理
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
parser.add_argument('--force-rebuild', action='store_true', help='全データを再生成')
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
print("麦生育マップ - NDVI/NDWI/GNDVI版 (キャッシュ機構)")
print("="*70)

# ===== 設定 =====
FIELD_ASSET = 'projects/ee-kitsukisaiseikyo/assets/2025442101'
TARGET_FIELDS_PATH = '新庄麦筆リスト.xlsx'
OUTPUT_DIR = 'output'
CACHE_DIR = os.path.join(OUTPUT_DIR, 'cache')
STATE_FILE = 'last_processed.txt'

START_DATE = '2024-12-01'
END_DATE = dt.datetime.now().strftime('%Y-%m-%d')
PIXEL_SCALE = 10
CLOUD_THRESHOLD = 20

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

print(f"\n前回処理日: {args.last_date}")
print(f"検索期間: {args.last_date} 〜 {END_DATE}")
print(f"雲量閾値: {CLOUD_THRESHOLD}%以下")
if args.force_rebuild:
    print("⚠️ 強制再構築モード")

# ===== データ読み込み =====
print("\n[1] データ読み込み中...")
target_fields_df = pd.read_excel(TARGET_FIELDS_PATH)
print(f"  ✓ 対象筆数: {len(target_fields_df)}筆")

field_polygons = ee.FeatureCollection(FIELD_ASSET)
target_polygon_ids = target_fields_df['polygon_uu'].tolist()
target_polygons = field_polygons.filter(ee.Filter.inList('polygon_uu', target_polygon_ids))

# ===== Sentinel-2取得 =====
print("\n[2] Sentinel-2画像検索中...")

def mask_s2_clouds(image):
    qa = image.select('QA60')
    mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
    return image.updateMask(mask).divide(10000)

def add_indices(image):
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    ndwi = image.normalizedDifference(['B8', 'B11']).rename('NDWI')
    gndvi = image.normalizedDifference(['B8', 'B3']).rename('GNDVI')
    return image.addBands([ndvi, ndwi, gndvi])

s2_collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
    .filterBounds(target_polygons.geometry())
    .filterDate(args.last_date if not args.force_rebuild else START_DATE, END_DATE)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', CLOUD_THRESHOLD))
    .map(mask_s2_clouds)
    .map(add_indices)
)

image_count = s2_collection.size().getInfo()
print(f"  ✓ 検索画像数: {image_count}枚")

if image_count == 0 and not args.force_rebuild:
    print("\n⚠️ 新規画像なし。処理をスキップします。")
    exit(0)

# ===== 履歴管理 =====
print("\n[3] 履歴データ読み込み中...")

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

# ===== 観測日取得 =====
print("\n[4] 観測日取得中...")

collection_info = s2_collection.getInfo()
all_dates_from_gee = {}

for feature in collection_info.get('features', []):
    props = feature.get('properties', {})
    if 'system:index' not in props:
        continue
    idx = props['system:index']
    date_obj = dt.datetime.strptime(idx[:8], '%Y%m%d')
    date_str = date_obj.strftime('%Y-%m-%d')
    all_dates_from_gee[date_str] = idx

# 新規日付と既存日付を分類
new_dates = []
existing_dates = []

for date_str, idx in sorted(all_dates_from_gee.items()):
    cache_file = os.path.join(CACHE_DIR, f'{date_str}.json')
    
    if args.force_rebuild or not os.path.exists(cache_file):
        new_dates.append(date_str)
        history['date_to_index'][date_str] = idx
    else:
        existing_dates.append(date_str)
        if date_str not in history['date_to_index']:
            history['date_to_index'][date_str] = idx

print(f"  ✓ 新規処理日数: {len(new_dates)}日")
print(f"  ✓ キャッシュ利用: {len(existing_dates)}日")

if len(new_dates) == 0 and not args.force_rebuild:
    print("\n⚠️ 処理対象の新規日付なし。")
    exit(0)

# ===== マップ中心座標 =====
print("\n[5] 筆ポリゴン情報取得中...")
fields_info = target_polygons.getInfo()
coords = target_polygons.geometry().bounds().getInfo()['coordinates'][0]
center_lon = sum([c[0] for c in coords]) / len(coords)
center_lat = sum([c[1] for c in coords]) / len(coords)
print(f"  ✓ マップ中心: ({center_lat:.4f}, {center_lon:.4f})")

# ===== カラーマップ関数 =====
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

def get_ndwi_color(ndwi):
    if ndwi is None or np.isnan(ndwi):
        return '#808080'
    if ndwi < -0.3:
        return '#8B4513'
    if ndwi < -0.1:
        return '#D2691E'
    if ndwi < 0.1:
        return '#F4A460'
    if ndwi < 0.3:
        return '#87CEEB'
    return '#4169E1'

def get_gndvi_color(gndvi):
    if gndvi is None or np.isnan(gndvi):
        return '#808080'
    if gndvi < 0.2:
        return '#FFFF00'
    if gndvi < 0.4:
        return '#9ACD32'
    if gndvi < 0.6:
        return '#32CD32'
    if gndvi < 0.8:
        return '#228B22'
    return '#006400'

# ===== 新規日付の処理とキャッシュ生成 =====
print("\n[6] 新規日付処理中...")

for date_idx, date in enumerate(new_dates):
    print(f"\n  === [{date_idx+1}/{len(new_dates)}] {date} 処理中 ===")
    
    target_index = history['date_to_index'][date]
    target_image = s2_collection.filter(ee.Filter.eq('system:index', target_index)).first()
    
    # 日付ごとのGeoJSONデータ
    date_cache = {
        'date': date,
        'fields': []
    }
    
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
            sample_data = target_image.select(['NDVI', 'NDWI', 'GNDVI']).sample(
                region=field_geom,
                scale=PIXEL_SCALE,
                geometries=True
            ).getInfo()
            
            if 'features' not in sample_data:
                print(" データなし")
                continue
            
            pixel_count = len(sample_data['features'])
            
            # 圃場データをキャッシュに保存
            field_data = {
                'polygon_uu': polygon_uu,
                'address': address,
                'boundary': feature['geometry']['coordinates'][0],
                'pixels': []
            }
            
            for pixel_feature in sample_data['features']:
                geom = pixel_feature.get('geometry', {})
                props = pixel_feature.get('properties', {})
                if not geom or not props:
                    continue
                
                lon, lat = geom['coordinates']
                field_data['pixels'].append({
                    'lat': lat,
                    'lon': lon,
                    'ndvi': props.get('NDVI'),
                    'ndwi': props.get('NDWI'),
                    'gndvi': props.get('GNDVI')
                })
            
            date_cache['fields'].append(field_data)
            date_pixels += pixel_count
            print(f" {pixel_count}px")
            
        except Exception as e:
            print(f" エラー: {e}")
            continue
    
    # キャッシュファイル保存
    cache_file = os.path.join(CACHE_DIR, f'{date}.json')
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(date_cache, f, ensure_ascii=False, indent=2)
    
    if date not in history['dates']:
        history['dates'].append(date)
    history['pixel_counts'][date] = date_pixels
    
    print(f"  ✓ {date}: {date_pixels}ピクセル (キャッシュ保存)")

# ===== マップ構築（全日付のキャッシュから） =====
print("\n[7] マップ構築中...")

m_ndvi = folium.Map(location=[center_lat, center_lon], zoom_start=15, tiles='OpenStreetMap')
m_ndwi = folium.Map(location=[center_lat, center_lon], zoom_start=15, tiles='OpenStreetMap')
m_gndvi = folium.Map(location=[center_lat, center_lon], zoom_start=15, tiles='OpenStreetMap')

all_dates = sorted(history['dates'])
total_pixels = 0

for date_idx, date in enumerate(all_dates):
    cache_file = os.path.join(CACHE_DIR, f'{date}.json')
    
    if not os.path.exists(cache_file):
        print(f"  ⚠️ キャッシュなし: {date}")
        continue
    
    print(f"  [{date_idx+1}/{len(all_dates)}] {date} 読み込み中...", end='', flush=True)
    
    with open(cache_file, 'r', encoding='utf-8') as f:
        date_cache = json.load(f)
    
    # レイヤー作成（最新日付のみ表示）
    show_layer = (date == all_dates[-1])
    layer_ndvi = FeatureGroup(name=f'NDVI_{date}', show=show_layer)
    layer_ndwi = FeatureGroup(name=f'NDWI_{date}', show=show_layer)
    layer_gndvi = FeatureGroup(name=f'GNDVI_{date}', show=show_layer)
    
    date_pixel_count = 0
    
    for field_data in date_cache['fields']:
        address = field_data['address']
        
        # ピクセル描画
        for pixel in field_data['pixels']:
            lat = pixel['lat']
            lon = pixel['lon']
            ndvi = pixel['ndvi']
            ndwi = pixel['ndwi']
            gndvi = pixel['gndvi']
            
            half_size = PIXEL_SCALE / 2 / 111320
            bounds = [[lat - half_size, lon - half_size], [lat + half_size, lon + half_size]]
            
            # NDVI
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
            
            # NDWI
            ndwi_str = f"{ndwi:.3f}" if ndwi is not None and not np.isnan(ndwi) else 'N/A'
            folium.Rectangle(
                bounds=bounds,
                color=get_ndwi_color(ndwi),
                fill=True,
                fillColor=get_ndwi_color(ndwi),
                fillOpacity=0.8,
                weight=0.5,
                popup=f"<b>{address}</b><br>日付: {date}<br>NDWI: {ndwi_str}",
                tooltip=f"{date}: NDWI {ndwi_str}"
            ).add_to(layer_ndwi)
            
            # GNDVI
            gndvi_str = f"{gndvi:.3f}" if gndvi is not None and not np.isnan(gndvi) else 'N/A'
            folium.Rectangle(
                bounds=bounds,
                color=get_gndvi_color(gndvi),
                fill=True,
                fillColor=get_gndvi_color(gndvi),
                fillOpacity=0.8,
                weight=0.5,
                popup=f"<b>{address}</b><br>日付: {date}<br>GNDVI: {gndvi_str}",
                tooltip=f"{date}: GNDVI {gndvi_str}"
            ).add_to(layer_gndvi)
            
            date_pixel_count += 1
        
        # 筆境界線
        coords_poly = [[lat, lon] for lon, lat in field_data['boundary']]
        folium.Polygon(coords_poly, color='#000000', weight=2, fill=False).add_to(layer_ndvi)
        folium.Polygon(coords_poly, color='#000000', weight=2, fill=False).add_to(layer_ndwi)
        folium.Polygon(coords_poly, color='#000000', weight=2, fill=False).add_to(layer_gndvi)
    
    layer_ndvi.add_to(m_ndvi)
    layer_ndwi.add_to(m_ndwi)
    layer_gndvi.add_to(m_gndvi)
    
    total_pixels += date_pixel_count
    print(f" {date_pixel_count}px")

# ===== LayerControl追加 =====
folium.LayerControl(position='topright', collapsed=False).add_to(m_ndvi)
folium.LayerControl(position='topright', collapsed=False).add_to(m_ndwi)
folium.LayerControl(position='topright', collapsed=False).add_to(m_gndvi)

# ===== レイヤー操作ボタン =====
layer_control_script = '''
<div id="layerButtons" style="position: fixed; bottom: 10px; right: 10px; z-index: 1000;
    background: white; padding: 8px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.3);">
  <button onclick="selectAllLayers()" style="display: block; width: 100%; margin-bottom: 4px;
    padding: 6px 12px; font-size: 13px; background: #3498db; color: white; border: none;
    border-radius: 4px; cursor: pointer;">全選択</button>
  <button onclick="deselectAllLayers()" style="display: block; width: 100%;
    padding: 6px 12px; font-size: 13px; background: #95a5a6; color: white; border: none;
    border-radius: 4px; cursor: pointer;">全解除</button>
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
</script>
'''
m_ndvi.get_root().html.add_child(folium.Element(layer_control_script))
m_ndwi.get_root().html.add_child(folium.Element(layer_control_script))
m_gndvi.get_root().html.add_child(folium.Element(layer_control_script))

# ===== タイトル・凡例追加（省略：元のコードと同じ） =====
# [タイトルと凡例のコードは元のコードをそのまま使用]
# ===== タイトル =====
all_dates = sorted(history['dates'])
total_pixels = sum(history['pixel_counts'].values())

title_ndvi = f'''
<div id="map-title-ndvi" style="position: fixed; top: 10px; left: 10px;
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            border: 2px solid white; z-index: 9999; padding: 10px;
            border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3); color: white;
            max-width: calc(100vw - 20px); box-sizing: border-box;">
    <h3 style="margin: 0; font-size: clamp(14px, 4vw, 20px);">🌾 NDVI マップ（植生活性度）</h3>
    <p style="margin: 5px 0 0 0; font-size: clamp(10px, 2.5vw, 13px); opacity: 0.9; line-height: 1.4;">
        📅 {all_dates[0]} 〜 {all_dates[-1]} ({len(all_dates)}日)<br>
        📍 {len(fields_info['features'])}筆 | 🔲 {total_pixels:,}px<br>
        🆕 {new_dates[-1]} | ☁️ {CLOUD_THRESHOLD}%以下
    </p>
</div>
<style>
@media (max-width: 768px) {{
    #map-title-ndvi {{
        left: 5px !important;
        top: 5px !important;
        padding: 8px !important;
        max-width: calc(100vw - 10px) !important;
    }}
    #map-title-ndvi h3 {{
        font-size: 12px !important;
    }}
    #map-title-ndvi p {{
        font-size: 9px !important;
    }}
}}
</style>
'''
m_ndvi.get_root().html.add_child(folium.Element(title_ndvi))

title_ndwi = f'''
<div id="map-title-ndwi" style="position: fixed; top: 10px; left: 10px;
            background: linear-gradient(135deg, #4169E1 0%, #87CEEB 100%);
            border: 2px solid white; z-index: 9999; padding: 10px;
            border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3); color: white;
            max-width: calc(100vw - 20px); box-sizing: border-box;">
    <h3 style="margin: 0; font-size: clamp(14px, 4vw, 20px);">💧 NDWI マップ（水分状態）</h3>
    <p style="margin: 5px 0 0 0; font-size: clamp(10px, 2.5vw, 13px); opacity: 0.9; line-height: 1.4;">
        📅 {all_dates[0]} 〜 {all_dates[-1]} ({len(all_dates)}日)<br>
        📍 {len(fields_info['features'])}筆 | 🔲 {total_pixels:,}px<br>
        🆕 {new_dates[-1]} | ☁️ {CLOUD_THRESHOLD}%以下
    </p>
</div>
<style>
@media (max-width: 768px) {{
    #map-title-ndwi {{
        left: 5px !important;
        top: 5px !important;
        padding: 8px !important;
        max-width: calc(100vw - 10px) !important;
    }}
    #map-title-ndwi h3 {{
        font-size: 12px !important;
    }}
    #map-title-ndwi p {{
        font-size: 9px !important;
    }}
}}
</style>
'''
m_ndwi.get_root().html.add_child(folium.Element(title_ndwi))

title_gndvi = f'''
<div id="map-title-gndvi" style="position: fixed; top: 10px; left: 10px;
            background: linear-gradient(135deg, #228B22 0%, #32CD32 100%);
            border: 2px solid white; z-index: 9999; padding: 10px;
            border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3); color: white;
            max-width: calc(100vw - 20px); box-sizing: border-box;">
    <h3 style="margin: 0; font-size: clamp(14px, 4vw, 20px);">🍃 GNDVI マップ（クロロフィル）</h3>
    <p style="margin: 5px 0 0 0; font-size: clamp(10px, 2.5vw, 13px); opacity: 0.9; line-height: 1.4;">
        📅 {all_dates[0]} 〜 {all_dates[-1]} ({len(all_dates)}日)<br>
        📍 {len(fields_info['features'])}筆 | 🔲 {total_pixels:,}px<br>
        🆕 {new_dates[-1]} | ☁️ {CLOUD_THRESHOLD}%以下
    </p>
</div>
<style>
@media (max-width: 768px) {{
    #map-title-gndvi {{
        left: 5px !important;
        top: 5px !important;
        padding: 8px !important;
        max-width: calc(100vw - 10px) !important;
    }}
    #map-title-gndvi h3 {{
        font-size: 12px !important;
    }}
    #map-title-gndvi p {{
        font-size: 9px !important;
    }}
}}
</style>
'''
m_gndvi.get_root().html.add_child(folium.Element(title_gndvi))

# ===== 凡例 =====
legend_ndvi = '''
<div id="map-legend" style="position: fixed; bottom: 10px; left: 10px;
            background-color: white; border: 2px solid #2c3e50; z-index: 9999;
            padding: 10px; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3);">
<h4 style="margin:0 0 8px 0; border-bottom:2px solid #3498db; padding-bottom:3px; font-size: clamp(12px, 3vw, 16px);">NDVI（植生）</h4>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#d73027; font-size: clamp(14px, 3.5vw, 20px);">■</span> 低 (&lt;0.2)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#fc8d59; font-size: clamp(14px, 3.5vw, 20px);">■</span> やや低 (0.2-0.4)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#fee08b; font-size: clamp(14px, 3.5vw, 20px);">■</span> 中 (0.4-0.6)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#91cf60; font-size: clamp(14px, 3.5vw, 20px);">■</span> 高 (0.6-0.8)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#1a9850; font-size: clamp(14px, 3.5vw, 20px);">■</span> 非常に高 (&gt;0.8)</p>
</div>
<style>
@media (max-width: 768px) {
    #map-legend {
        bottom: 5px !important;
        left: 5px !important;
        padding: 6px !important;
        max-width: 120px !important;
    }
    #map-legend h4 {
        font-size: 11px !important;
        margin-bottom: 5px !important;
    }
    #map-legend p {
        font-size: 9px !important;
        margin: 2px 0 !important;
    }
    #map-legend span {
        font-size: 14px !important;
    }
}
</style>
'''
legend_ndvi = '''
<div id="map-legend" style="position: fixed; bottom: 10px; left: 10px;
            background-color: white; border: 2px solid #2c3e50; z-index: 9999;
            padding: 10px; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3);">
<h4 style="margin:0 0 8px 0; border-bottom:2px solid #3498db; padding-bottom:3px; font-size: clamp(12px, 3vw, 16px);">NDVI（植生）</h4>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#d73027; font-size: clamp(14px, 3.5vw, 20px);">■</span> 低 (&lt;0.2)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#fc8d59; font-size: clamp(14px, 3.5vw, 20px);">■</span> やや低 (0.2-0.4)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#fee08b; font-size: clamp(14px, 3.5vw, 20px);">■</span> 中 (0.4-0.6)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#91cf60; font-size: clamp(14px, 3.5vw, 20px);">■</span> 高 (0.6-0.8)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#1a9850; font-size: clamp(14px, 3.5vw, 20px);">■</span> 非常に高 (&gt;0.8)</p>
</div>
<style>
@media (max-width: 768px) {
    #map-legend {
        bottom: 5px !important;
        left: 5px !important;
        padding: 6px !important;
        max-width: 110px !important;
    }
    #map-legend h4 {
        font-size: 10px !important;
        margin-bottom: 4px !important;
    }
    #map-legend p {
        font-size: 8px !important;
        margin: 1px 0 !important;
    }
    #map-legend span {
        font-size: 12px !important;
    }
}
</style>
'''
m_ndvi.get_root().html.add_child(folium.Element(legend_ndvi))

legend_ndwi = '''
<div id="map-legend" style="position: fixed; bottom: 10px; left: 10px;
            background-color: white; border: 2px solid #2c3e50; z-index: 9999;
            padding: 10px; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3);">
<h4 style="margin:0 0 8px 0; border-bottom:2px solid #3498db; padding-bottom:3px; font-size: clamp(12px, 3vw, 16px);">NDWI（水分）</h4>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#8B4513; font-size: clamp(14px, 3.5vw, 20px);">■</span> 乾燥 (&lt;-0.3)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#D2691E; font-size: clamp(14px, 3.5vw, 20px);">■</span> やや乾燥 (-0.3~-0.1)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#F4A460; font-size: clamp(14px, 3.5vw, 20px);">■</span> 適度 (-0.1~0.1)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#87CEEB; font-size: clamp(14px, 3.5vw, 20px);">■</span> 湿潤 (0.1~0.3)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#4169E1; font-size: clamp(14px, 3.5vw, 20px);">■</span> 多湿 (&gt;0.3)</p>
</div>
<style>
@media (max-width: 768px) {
    #map-legend {
        bottom: 5px !important;
        left: 5px !important;
        padding: 6px !important;
        max-width: 110px !important;
    }
    #map-legend h4 {
        font-size: 10px !important;
        margin-bottom: 4px !important;
    }
    #map-legend p {
        font-size: 8px !important;
        margin: 1px 0 !important;
    }
    #map-legend span {
        font-size: 12px !important;
    }
}
</style>
'''
m_ndwi.get_root().html.add_child(folium.Element(legend_ndwi))

legend_gndvi = '''
<div id="map-legend" style="position: fixed; bottom: 10px; left: 10px;
            background-color: white; border: 2px solid #2c3e50; z-index: 9999;
            padding: 10px; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3);">
<h4 style="margin:0 0 8px 0; border-bottom:2px solid #3498db; padding-bottom:3px; font-size: clamp(12px, 3vw, 16px);">GNDVI（クロロフィル）</h4>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#FFFF00; font-size: clamp(14px, 3.5vw, 20px);">■</span> 低 (&lt;0.2)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#9ACD32; font-size: clamp(14px, 3.5vw, 20px);">■</span> やや低 (0.2-0.4)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#32CD32; font-size: clamp(14px, 3.5vw, 20px);">■</span> 中 (0.4-0.6)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#228B22; font-size: clamp(14px, 3.5vw, 20px);">■</span> 高 (0.6-0.8)</p>
<p style="margin:3px 0; font-size: clamp(10px, 2.5vw, 14px);"><span style="color:#006400; font-size: clamp(14px, 3.5vw, 20px);">■</span> 非常に高 (&gt;0.8)</p>
</div>
<style>
@media (max-width: 768px) {
    #map-legend {
        bottom: 5px !important;
        left: 5px !important;
        padding: 6px !important;
        max-width: 110px !important;
    }
    #map-legend h4 {
        font-size: 10px !important;
        margin-bottom: 4px !important;
    }
    #map-legend p {
        font-size: 8px !important;
        margin: 1px 0 !important;
    }
    #map-legend span {
        font-size: 12px !important;
    }
}
</style>
'''
m_gndvi.get_root().html.add_child(folium.Element(legend_gndvi))

# ===== 保存 =====
print("\n[8] マップ保存中...")

m_ndvi.save(os.path.join(OUTPUT_DIR, 'index.html'))
m_ndwi.save(os.path.join(OUTPUT_DIR, 'ndwi.html'))
m_gndvi.save(os.path.join(OUTPUT_DIR, 'gndvi.html'))

print(f"  ✓ NDVIマップ: index.html")
print(f"  ✓ NDWIマップ: ndwi.html")
print(f"  ✓ GNDVIマップ: gndvi.html")

# ===== 履歴保存 =====
with open(history_file, 'w', encoding='utf-8') as f:
    json.dump(history, f, ensure_ascii=False, indent=2)

if new_dates:
    with open(STATE_FILE, 'w') as f:
        f.write(new_dates[-1])
    print(f"  ✓ 最終処理日: {new_dates[-1]}")

print("\n" + "="*70)
print("✓ 更新完了！")
print("="*70)
print(f"\n新規処理: {len(new_dates)}日")
print(f"キャッシュ利用: {len(existing_dates)}日")
print(f"総観測日数: {len(all_dates)}日")
print(f"総ピクセル数: {total_pixels:,}")
print("="*70)

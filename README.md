# 🌾 麦生育マップ自動更新システム

Sentinel-2衛星画像を使った麦生育モニタリングシステム

## 📊 マップを見る

### 3つの植生指標マップ
- **[NDVIマップ（植生活性度）](https://kitsukisaiseikyo-byte.github.io/mugimap-shinjo/)** 🌿  
  植物の光合成活性と生育状況を示します
  
- **[NDWIマップ（水分状態）](https://kitsukisaiseikyo-byte.github.io/mugimap-shinjo/ndwi.html)** 💧  
  作物の水分ストレス状態を把握できます
  
- **[GNDVIマップ（クロロフィル含量）](https://kitsukisaiseikyo-byte.github.io/mugimap-shinjo/gndvi.html)** 🍃  
  葉の健康状態とクロロフィル量を表示します

## 🔍 各指標の説明

### NDVI (正規化植生指数)
- **計算式**: (NIR - Red) / (NIR + Red)
- **用途**: 植生の活性度、バイオマス量の推定
- **値の範囲**: -1 〜 +1（高いほど健全な植生）

### NDWI (正規化水分指数)
- **計算式**: (NIR - SWIR) / (NIR + SWIR)
- **用途**: 作物の水分ストレス検出、灌水管理
- **値の範囲**: -1 〜 +1（高いほど水分量が多い）

### GNDVI (緑色正規化植生指数)
- **計算式**: (NIR - Green) / (NIR + Green)
- **用途**: クロロフィル含量、窒素栄養状態の評価
- **値の範囲**: -1 〜 +1（高いほどクロロフィルが豊富）

## 🔄 更新頻度

**毎日午前2時（JST）に自動チェック**  
新規Sentinel-2画像（雲量50%以下）がある場合のみ更新・・・25/11/21　データの取得率が悪かったので20％から変更

## 🛠️ 技術仕様

### 使用データ
- **衛星**: Sentinel-2 (ESA)
- **解像度**: 10m
- **バンド**: B3(Green), B4(Red), B8(NIR), B11(SWIR)
- **雲量閾値**: 20%以下

### 技術スタック
- **衛星画像処理**: Google Earth Engine
- **地図表示**: Folium (Leaflet.js)
- **自動化**: GitHub Actions
- **言語**: Python 3.10

### 主要ライブラリ
```
earthengine-api
folium
pandas
numpy
openpyxl
```

## 📁 ファイル構成

```
mugimap-shinjo/
├── generate_maps.py          # メインスクリプト（3マップ生成）
├── 新庄麦筆リスト.xlsx         # 対象圃場リスト
├── .github/workflows/
│   └── update-maps.yml       # 自動更新ワークフロー
├── output/
│   ├── index.html            # NDVIマップ
│   ├── ndwi.html             # NDWIマップ
│   ├── gndvi.html            # GNDVIマップ
│   └── observation_history.json
└── README.md
```

## 🚀 セットアップ方法

### 1. リポジトリのクローン
```bash
git clone https://github.com/kitsukisaiseikyo-byte/mugimap-shinjo.git
cd mugimap-shinjo
```

### 2. Google Earth Engine認証設定
GitHub Secretsに以下を登録：
- `GEE_SERVICE_ACCOUNT`: サービスアカウントメール
- `GEE_PRIVATE_KEY`: 秘密鍵JSON（Base64エンコード推奨）

### 3. GitHub Pagesを有効化
Settings → Pages → Source: `gh-pages` branch

## 🔧 ローカル実行

```bash
# 依存関係インストール
pip install earthengine-api pandas openpyxl folium numpy

# 認証設定（初回のみ）
earthengine authenticate

# マップ生成
python generate_maps.py --last-date 2024-12-01
```

## 📈 使い方

### マップの見方
1. 各マップページにアクセス
2. 右上のレイヤーコントロールで観測日を選択
3. ピクセルをクリックすると詳細情報が表示

### 複数日の比較
- 「全選択」ボタンで全日付のレイヤーを表示
- 時系列での変化を確認可能

## 📝 データ活用例

### 栽培管理への応用
- **NDVI**: 生育ステージの把握、追肥タイミングの決定
- **NDWI**: 灌水の要否判断、干ばつストレスの早期発見
- **GNDVI**: 窒素栄養診断、病害の早期検出

### 圃場間比較
- 同一日付で複数圃場の状態を比較
- 生育のばらつきを可視化

## 🤝 貢献

バグ報告や機能追加の提案は[Issues](https://github.com/kitsukisaiseikyo-byte/mugimap-shinjo/issues)へ

## 📄 ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 🙏 謝辞

- 衛星画像: ESA Copernicus Programme (Sentinel-2)
- 画像処理プラットフォーム: Google Earth Engine
- 地図ライブラリ: Folium / Leaflet.js

---

**Last Updated**: 2025-11-05  

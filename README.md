# マート改修AIツール

Google Gemini APIを使用して、データウェアハウスのマート（SQLファイル）を自動改修するツールです。

## 概要

このツールは、自然言語による改修要望を受け取り、以下の処理を自動で実行します：

1. **マートルーター**: `meta_data.json`に登録されたマート情報を参照し、改修対象のSQLファイルを選択
2. **マートエディター**: 選択されたSQLファイルを読み込み、要望に沿ってSQLを自動改修

## セットアップ

### 1. 仮想環境の作成と有効化

```bash
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# または
venv\Scripts\activate  # Windows
```

### 2. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 3. 環境変数の設定

Google Gemini APIキーを環境変数に設定します：

```bash
export GOOGLE_API_KEY="あなたのAPIキー"
```

または、`.env`ファイルを作成して設定することもできます（その場合は`python-dotenv`の追加が必要です）。

## 使用方法

1. 仮想環境を有効化します
2. 環境変数`GOOGLE_API_KEY`が設定されていることを確認します
3. `main.py`を実行します：

```bash
python main.py
```

4. プロンプトが表示されたら、マート改修要望を日本語で入力します

例：
```
売上マートに粗利列を追加したい
```

5. ツールが自動的に以下を実行します：
   - 改修対象のSQLファイルを選択
   - SQLファイルを読み込み
   - 要望に沿ってSQLを改修
   - 改修後のSQLをファイルに書き込み

## プロジェクト構造

```
demo/
├── main.py                          # メインスクリプト
├── meta_data.json                   # マートメタデータ（path, description）
├── requirements.txt                 # 依存パッケージ
├── prompts/
│   ├── mart_router_system_prompt.md # マートルーター用プロンプト
│   └── mart_editor_system_prompt.md # マートエディター用プロンプト
└── sql/
    ├── revenue.sql                  # 売上マートSQL
    ├── office.sql                   # オフィスマスタSQL
    └── cost.sql                     # コストマートSQL
```

## マートメタデータの管理

`meta_data.json`にマート情報を登録することで、新しいマートも自動的に認識されます。

```json
[
  {
    "path": "sql/revenue.sql",
    "description": "売上情報を集計したファクトテーブル..."
  }
]
```

## 注意事項

- このツールは既存のマートを改修することを想定しています
- 新規マートの作成には対応していません
- SQLファイルは自動的に上書きされます。実行前にバックアップを推奨します
- 改修内容は必ず確認してから本番環境に適用してください

## トラブルシューティング

### JSON解析エラーが発生する場合

Gemini APIのレスポンスにマークダウンのコードブロックが含まれている場合、自動的に抽出されますが、エラーが発生した場合はレスポンス内容が表示されます。

### ファイルが見つからない場合

`meta_data.json`の`path`が正しいことを確認してください。パスは`main.py`からの相対パスで指定します。

## ライセンス

個人学習用です

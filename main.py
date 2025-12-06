import google.generativeai as genai  # Googleの生成AIライブラリ
import os
import json
import re

# APIキーの設定（環境変数から取得する方法）
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# モデルの選択
model = genai.GenerativeModel("models/gemini-2.0-flash-001")

# GenerationConfigでパラメータを設定
config = genai.GenerationConfig(
    max_output_tokens=2048,  # 生成されるトークンの最大数
    temperature=0.8,  # 出力のランダム性を制御
)

# JSONを抽出する関数
def extract_json(text):
    """レスポンスからJSON部分を抽出する"""
    # コードブロック（```json ... ```）を削除
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    text = text.strip()
    
    # JSONオブジェクトを探す（{ ... }）
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
    if json_match:
        return json_match.group(0)
    return text

#　改修マート選択ルーター
def mart_router(model, user_input):
    # meta_data.jsonを読み込む
    # MCPでgithubのマートメタデータを参照する
    with open("meta_data.json", "r", encoding="utf-8") as f:
        marts = json.load(f)
    
    system_prompt = open("prompts/mart_router_system_prompt.md", "r", encoding="utf-8").read()
    
    # user_requestとmartsをJSON形式でプロンプトに含める
    user_prompt = json.dumps({
        "user_request": user_input,
        "marts": marts
    }, ensure_ascii=False, indent=2)
    
    prompt = system_prompt + "\n\n" + user_prompt
    response = model.generate_content(prompt, generation_config=config)
    return response.text

#　改修マートエディター
def mart_editor(model, user_request, target_path, original_sql):
    system_prompt = open("prompts/mart_editor_system_prompt.md", "r").read()
    # JSON形式でuser_request、target_path、original_sqlを渡す
    user_prompt = json.dumps({
        "user_request": user_request,
        "target_path": target_path,
        "original_sql": original_sql
    }, ensure_ascii=False)
    prompt = system_prompt + "\n\n" + user_prompt
    # MCPでgithubの過去の改修を参照とかすれば精度良くなるかも
    response = model.generate_content(prompt, generation_config=config)
    return response.text

user_input = input("マート改修要望を入力してください: ")
response = mart_router(model, user_input)

# JSONを抽出してパース
try:
    json_text = extract_json(response)
    result = json.loads(json_text)
    modify_sql_path = result["selected_path"]
    
    # selected_pathがnullの場合は処理を終了
    if modify_sql_path is None:
        print(f"マート改修対象が選択されませんでした: {result.get('reason_ja', '')}")
    else:
        # SQLファイルを読み込む
        # ここもMCPでgithubのSQLファイルを参照する
        modify_sql = open(modify_sql_path, "r", encoding="utf-8").read()
        
        # マートエディターでSQLを改修
        editor_response = mart_editor(model, user_input, modify_sql_path, modify_sql)
        
        # エディターのレスポンスからJSONを抽出
        editor_json_text = extract_json(editor_response)
        editor_result = json.loads(editor_json_text)
        
        # statusがokの場合のみファイルを更新
        if editor_result.get("status") == "ok":
            modified_sql = editor_result["modified_sql"]
            # ファイルに書き込む
            with open(modify_sql_path, "w", encoding="utf-8") as f:
                f.write(modified_sql)
            print(f"✓ {modify_sql_path} を更新しました")
            print(f"コメント: {editor_result.get('comment_ja', '')}")
        else:
            print(f"✗ マート改修に失敗しました: {editor_result.get('reason_ja', '')}")
    
except json.JSONDecodeError as e:
    print(f"JSON解析エラー: {e}")
    print(f"レスポンス内容:\n{response}")
except KeyError as e:
    print(f"キーエラー: {e}")
    print(f"レスポンス内容:\n{response}")
except FileNotFoundError as e:
    print(f"ファイルが見つかりません: {e}")
except Exception as e:
    print(f"エラーが発生しました: {e}")
    import traceback
    traceback.print_exc()

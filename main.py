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
    max_output_tokens=1000,  # 生成されるトークンの最大数
    temperature=0,  # 出力のランダム性を制御
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

# 改修プランナー
def mart_edit_planner(model, user_request, target_path, original_sql):
    system_prompt = open("prompts/mart_edit_planner_system_prompt.md", "r").read()
    user_prompt = json.dumps({
        "user_request": user_request,
        "target_path": target_path,
        "original_sql": original_sql
    }, ensure_ascii=False)
    prompt = system_prompt + "\n\n" + user_prompt
    response = model.generate_content(prompt, generation_config=config)
    return response.text

#　改修マートエディター
def mart_editor(model, plan_md, target_path, original_sql):
    system_prompt = open("prompts/mart_editor_system_prompt.md", "r").read()
    # JSON形式でplan_md、target_path、original_sqlを渡す
    user_prompt = json.dumps({
        "plan_md": plan_md,
        "target_path": target_path,
        "original_sql": original_sql
    }, ensure_ascii=False)
    prompt = system_prompt + "\n\n" + user_prompt
    # MCPでgithubの過去の改修を参照とかすれば精度良くなるかも
    response = model.generate_content(prompt, generation_config=config)
    return response.text


session = {
    "phase": "initial",  # initial | plan_review
    "user_request": None,
    "plan_md": None,
    "target_path": None,
    "original_sql": None,
}

print("q を入力すると終了します。")

while True:
    try:
        if session["phase"] == "initial":
            user_input = input("マート改修要望を入力してください: ").strip()
            if user_input.lower() in {"q", "quit", "exit"}:
                break

            session["user_request"] = user_input
            response = mart_router(model, user_input)

            json_text = extract_json(response)
            result = json.loads(json_text)
            modify_sql_path = result["selected_path"]

            if modify_sql_path is None:
                print(f"マート改修対象が選択されませんでした: {result.get('reason_ja', '')}")
                continue

            session["target_path"] = modify_sql_path
            session["original_sql"] = open(modify_sql_path, "r", encoding="utf-8").read()

            plan_response = mart_edit_planner(
                model, session["user_request"], modify_sql_path, session["original_sql"]
            )
            session["plan_md"] = plan_response
            session["phase"] = "plan_review"

            print("▼ 改修プラン案")
            print(plan_response)
            print("\nプランへの追加要望があれば入力してください。実装OKなら「OK」「進めて」などと入力してください。")

        elif session["phase"] == "plan_review":
            feedback = input("プランへのコメント/同意を入力してください: ").strip()
            if feedback.lower() in {"q", "quit", "exit"}:
                break

            is_accept = feedback.lower() in {"ok", "進めて", "大丈夫", "はい", "実装して"}

            if is_accept:
                editor_response = mart_editor(
                    model,
                    session["plan_md"],
                    session["target_path"],
                    session["original_sql"],
                )
                editor_json_text = extract_json(editor_response)
                editor_result = json.loads(editor_json_text)

                if editor_result.get("status") == "ok":
                    with open(session["target_path"], "w", encoding="utf-8") as f:
                        f.write(editor_result["modified_sql"])
                    print(f"✓ {session['target_path']} を更新しました")
                    print(f"コメント: {editor_result.get('comment_ja', '')}")
                else:
                    print(f"✗ マート改修に失敗しました: {editor_result.get('reason_ja', '')}")

                session["phase"] = "initial"
                session["user_request"] = None
                session["plan_md"] = None
                session["target_path"] = None
                session["original_sql"] = None

            else:
                merged_request = f"{session['user_request']}\n追加要望: {feedback}"
                session["user_request"] = merged_request
                plan_response = mart_edit_planner(
                    model,
                    session["user_request"],
                    session["target_path"],
                    session["original_sql"],
                )
                session["plan_md"] = plan_response
                print("▼ 改修プランを更新しました")
                print(plan_response)
                print("\nさらに要望があれば入力してください。問題なければ実装OKと入力してください。")

    except json.JSONDecodeError as e:
        print(f"JSON解析エラー: {e}")
        print("レスポンス内容:")
        print(response if 'response' in locals() else "")
        session["phase"] = "initial"
    except FileNotFoundError as e:
        print(f"ファイルが見つかりません: {e}")
        session["phase"] = "initial"
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback

        traceback.print_exc()
        session["phase"] = "initial"

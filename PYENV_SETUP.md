# pyenv-virtualenv を用いたセットアップ手順

このドキュメントでは、`pyenv` と `pyenv-virtualenv` を使用して本リポジトリ用の Python 仮想環境を構築する具体的な手順を説明します。

## 1. pyenv のインストール

macOS の場合は Homebrew を使用してインストールできます。

```bash
brew install pyenv
```

Linux (Ubuntu/Debian) では以下のようにリポジトリからインストールします。

```bash
curl https://pyenv.run | bash
```

インストール後、以下をシェルの設定ファイル (`~/.bashrc` や `~/.zshrc`) に追記して pyenv を初期化します。

```bash
export PYENV_ROOT="$HOME/.pyenv"
command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
```

シェルを再読み込みして `pyenv --version` が表示されることを確認してください。

## 2. pyenv-virtualenv のインストール

```bash
git clone https://github.com/pyenv/pyenv-virtualenv.git "$PYENV_ROOT/plugins/pyenv-virtualenv"
```

同様にシェル設定ファイルに以下を追記し、再読み込みします。

```bash
eval "$(pyenv virtualenv-init -)"
```

## 3. Python のインストールと仮想環境作成

本プロジェクトでは Python 3.11.12 を使用します。まだインストールしていない場合は次のコマンドで追加します。

```bash
pyenv install 3.11.12
```

続いて仮想環境を作成します。

```bash
pyenv virtualenv 3.11.12 gpt-trade
```

プロジェクトディレクトリ内で次のコマンドを実行し、ローカル環境として設定します。

```bash
pyenv local gpt-trade
```

これにより `.python-version` ファイルが作成され、ディレクトリに入るだけで仮想環境が自動的に有効化されます。

## 4. 依存パッケージのインストール

仮想環境が有効になっている状態で、依存パッケージをインストールします。

```bash
pip install -r requirements.txt
```

以上で初期セットアップは完了です。以降はこのディレクトリに入るだけで `gpt-trade` 環境がアクティブになります。

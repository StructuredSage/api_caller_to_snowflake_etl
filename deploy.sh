echo "commit id is - $1"
DIR="/linuxServerPath/snowflake"
if [ ! -d "$DIR" ]
then
    echo "$DIR does not exist, please set up the directory"
    exit 1
else
    echo "changing path to $DIR"
    cd $DIR
    git fetch
    # try checkout only if working tree is clean
    if [ -z "$(git status --untracked-files=no --porcelain)" ]; then
        git checkout $1
    else
        echo "There are uncommited changes on the server, please clean up before deploying"
        exit 1
    fi

    git checkout $1
    rm -r .venv
    /bin/python38 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
fi

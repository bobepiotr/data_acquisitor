:'
  This script installs some of acq_data_root.sh dependencies.
  Should be run before acq_data_root.sh.

  Example usage: ./root_install.sh
'

echo -ne '\n' | sudo apt-get install software-properties-common
echo -ne '\n' | sudo add-apt-repository ppa:deadsnakes/ppa
echo -ne '\n' | sudo apt-get update
echo -ne '\n' | sudo apt-get install python3.8
echo -ne '\n' | sudo apt-get install python3-pip
echo -ne '\n' | python3 -m pip install -r ./data_acquisitor/requirements.txt

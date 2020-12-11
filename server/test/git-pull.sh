REPOSITORY_ROOT=$1
FERRY_API_ROOT=$2
FERRY_API_EXECUTABLE=$3
FERRY_USER=$4

#echo "Renewing kerberos ticket."
#sudo -u ferry kinit -R
#if [ $? != 0 ]; then
#  echo "Failed to automatically renew kerberos ticket. Please, renew it manually."
#  exit 1
#fi

echo -e "\nUpdating Ferry repository."
cd $REPOSITORY_ROOT
PULL_OUT=$(sudo -u $FERRY_USER git pull 2>&1)
PULL_EXIT_CODE=$?
echo -e $PULL_OUT
if [ $PULL_EXIT_CODE != 0 ]; then
  echo "Failed to pull. Please, resolve the issue and try again."
  exit 2
fi
if [[ $PULL_OUT == "Already up-to-date." ]]; then
  exit 0
fi

echo -e "\nBuilding Ferry API."
cd $FERRY_API_ROOT
sudo -u $FERRY_USER go build
if [ $? != 0 ]; then
  echo "Failed to build Ferry API."
  exit 3
fi

echo -e "\nRestarting Ferry API."
systemctl restart ferry
systemctl status ferry
if [ $? != 0 ]; then
  echo "Failed to start Ferry API."
  exit 4
fi

echo -e "\nDone!"

#! /bin/bash

cd evo-manager-v2
npm install
npm run build
cd ..
rm -rf manager/dist
cp -r evo-manager-v2/dist manager/dist
cd venv/Lib/site-packages
zip ../../../vendorid-generator-service-package.zip -r .
cd ..
cd ..
cd ..
zip -g vendorid-generator-service-package.zip lambda_function.py
zip -g vendorid-generator-service-package.zip Models/*
  aws lambda update-function-code \
    --function-name  vendorid-generator-service \
    --zip-file fileb://vendorid-generator-service-package.zip \
    --region sa-east-1

#Impoerted Required Moules
from flask import Flask,request
#Flask Restex Framework
from flask_restx import Api,Resource


#Here we are creating a Flask App object
app = Flask(__name__);
#Here we are creating Api object imported from Restex Framework
api  = Api(app);



#This a class which is inheriting Resource Class

class HelloWord(Resource):

    @classmethod
    def get(cls):
        try:
            return {"msg":"Hello To Flask World!1","status":200},200
        except Exception as e:
            return {"msg":"Sorry For Issues!!","status":500},500

    @classmethod
    def post(cls):
        try:
            data= request.get_json();
            firstname = data['firstname'];
            lastname  = data['lastname'];
            message = "My name is"+firstname +" "+lastname
            return {"msg":message,"status":201},201
        except Exception as e:
            return {"msg":"Sorry For Issues!!","status":500},500

#Adding Route For Rest Apis
api.add_resource(HelloWord,"/hello");

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=9001, debug=True)



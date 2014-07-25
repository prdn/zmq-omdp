module.exports = {
   JSON: {
	   encode: function(data) {
		   try {
			   data = JSON.stringify(data);
		   } catch(e) {
			   this.emitErr('ERR_MSG_ENCODING', e);
		   }
		   return data;
	   },
	   decode: function(data) {
		   try {
			   data = JSON.parse(data);
		   } catch(e) {
			   this.emitErr('ERR_MSG_ENCODING', e);
		   }
		   return data;
	   }
	}
};

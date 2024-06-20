// src/components/modalConfig.tsx

const modalConfig = {
  customers: {
    options: {
      issue: ["A", "B", "C"]
    },
    conditional_options: {
      status: [
	{
	   condition: "issue == A",
	   options: ["X1","X2","X3"]
	},
        {
           condition: "issue == B",
           options: ["Y1","Y2","Y3"]
        },
        {
           condition: "issue == C",
           options: ["Z1","Z2","Z3"]
        }      
      ] 
    },
    scopes: {
      create: true,
      read: ["id", "mobile", "issue", "status", "created_at"],
      update: ["mobile","issue", "status"],
      delete: true
    },
    validation_rules: {
      "mobile": ["REQUIRED"]
    },
    ai_quality_checks: {
      "mobile": ["rhymes with potato", "is a fruit or vegetable"]
    },
  },
  partners: {
    options: {
      issue: ["A", "B", "C"],
      status: ["X", "Y"]
    },
    scopes: {
      create: true,
      read: ["id", "mobile", "issue", "status", "created_at"],
      update: ["issue", "status"],
      delete: false
    }
  }
};

export default modalConfig;


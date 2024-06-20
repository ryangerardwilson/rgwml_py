export const validateField = (field: string, value: any, rules: string[]): string | null => {
  for (const rule of rules) {
    const [ruleName, ...params] = rule.split(':');
    switch (ruleName) {
      case 'REQUIRED':
        if (!value) {
          return `${field} is required.`;
        }
        break;
      case 'CHAR_LENGTH':
        const length = parseInt(params[0], 10);
        if (value.length !== length) {
          return `${field} must be ${length} characters long.`;
        }
        break;
      case 'IS_NUMERICALLY_PARSEABLE':
        if (isNaN(Number(value))) {
          return `${field} must be numerically parseable.`;
        }
        break;
      case 'IS_INDIAN_MOBILE_NUMBER':
        const uniqueDigits = new Set(value.split('')).size;
        if (!/^[6789]\d{9}$/.test(value) || uniqueDigits < 4) {
          return `${field} must be a valid Indian mobile number.`;
        }
        break;
      case 'IS_YYYY-MM-DD':
        if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
          return `${field} must be in YYYY-MM-DD format.`;
        }
        break;
      case 'IS_AFTER_TODAY':
        if (new Date(value) <= new Date()) {
          return `${field} must be a date after today.`;
        }
        break;
      case 'IS_BEFORE_TODAY':
        if (new Date(value) >= new Date()) {
          return `${field} must be a date before today.`;
        }
        break;
      // Add more validation rules as needed
      default:
        break;
    }
  }
  return null;
};

const OPEN_AI_KEY = process.env.NEXT_PUBLIC_OPEN_AI_KEY;
const OPEN_AI_MODEL = process.env.NEXT_PUBLIC_OPEN_AI_JSON_MODE_MODEL;

export const open_ai_quality_checks = async (field: string, value: string, checks: string[]): Promise<string[]> => {
  const failedChecks: string[] = [];

  if (!OPEN_AI_KEY) {
    console.error("OpenAI API key is not set");
    return ["OpenAI API key is not set"];
  }

  for (const check of checks) {
    const prompt = `
    Your only job is to ascertain if the user's input meets this criterion '${check}' and output a boolean true or false, as JSON in this format {"evaluation": "true"}.
    `;

    try {
      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${OPEN_AI_KEY}`
        },
        body: JSON.stringify({
          model: OPEN_AI_MODEL,
          messages: [
            {
              role: 'system',
              content: prompt
            },
            {
              role: 'user',
              content: value
            }
          ]
        })
      });

      const data = await response.json();
      const aiResult = data.choices[0].message.content;
      const evaluation = JSON.parse(aiResult).evaluation;

      if (evaluation !== "true") {
        failedChecks.push(`${field} does not meet the criterion: ${check}`);
      }
    } catch (error) {
      console.error('Error during OpenAI API call:', error);
      failedChecks.push(`Error evaluating ${check}: ${error.message}`);
    }
  }

  return failedChecks;
};


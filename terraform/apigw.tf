############################################
# API Gateway (HTTP API v2) -> Lambda
############################################

resource "aws_apigatewayv2_api" "events_http_api" {
  name          = "${var.project_name}-http-api-${var.environment}"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["content-type", "authorization", "x-api-key", "x-amz-date", "x-amz-security-token"]
    max_age       = 3600
  }
}

resource "aws_apigatewayv2_integration" "events_lambda_integration" {
  api_id                 = aws_apigatewayv2_api.events_http_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.generate_events.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
  timeout_milliseconds   = 30000
}

# POST /generate -> Lambda
resource "aws_apigatewayv2_route" "generate_route" {
  api_id    = aws_apigatewayv2_api.events_http_api.id
  route_key = "POST /generate"
  target    = "integrations/${aws_apigatewayv2_integration.events_lambda_integration.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.events_http_api.id
  name        = "$default"
  auto_deploy = true
}

############################################
# Allow API Gateway to invoke Lambda
############################################

resource "aws_lambda_permission" "allow_apigw_invoke" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.generate_events.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.events_http_api.execution_arn}/*/*"
}

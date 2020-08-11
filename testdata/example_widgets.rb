require 'sinatra'
require 'json'
require 'csv'

get '/widgets.json' do
  halt 401 if request.env["HTTP_AUTHORIZATION"] != "Bearer de97ca4d5eacc3fed2ac3332"

  content_type :json

  data = File.open('example_widgets.csv').read
  {
    widgets: CSV.parse(data, headers: %w(id price ranking name active launched created_at description)).map(&:to_h)
  }.to_json
end

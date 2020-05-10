require 'faker'

srand(129801)

puts "MySQL - appdb"
puts "====="
puts

puts "CREATE TABLE IF NOT EXISTS users (id int8 not null auto_increment, email varchar(255), name varchar(255), company_id int8, updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, created_at timestamp DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(id));"

25.times do
  first_name = Faker::Name.first_name
  last_name = Faker::Name.last_name
  email = Faker::Internet.safe_email(name: "#{first_name}_#{last_name}".downcase)
  company_id = Faker::Number.number(digits: 5)
  puts %[INSERT INTO users (email, name, company_id) VALUES ("#{email}", "#{first_name} #{last_name}", #{company_id});]
end

srand(129801)

puts
puts "Postgres - crm"
puts "====="
puts

puts "CREATE TABLE IF NOT EXISTS companies (id int8 not null, company_name varchar(255), company_type varchar(255), industry varchar(255), description varchar(255), updated_at timestamp DEFAULT CURRENT_TIMESTAMP, created_at timestamp DEFAULT CURRENT_TIMESTAMP);"

25.times do
  company_id = Faker::Number.number(digits: 5)
  company_name = Faker::Company.name
  company_type = Faker::Company.type
  industry = Faker::Company.industry
  description = Faker::Company.bs
  puts %[INSERT INTO companies (id, company_name, company_type, industry, description) VALUES (#{company_id}, '#{company_name}', '#{company_type}', '#{industry}', '#{description}');]
end

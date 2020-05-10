require 'faker'

puts "CREATE TABLE IF NOT EXISTS users (id int8 not null auto_increment, email varchar(255), name varchar(255), customer_id int8, updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, created_at timestamp DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(id));"

25.times do
  first_name = Faker::Name.first_name
  last_name = Faker::Name.last_name
  email = Faker::Internet.safe_email(name: "#{first_name}_#{last_name}".downcase)
  customer_id = Faker::Number.number(digits: 5)
  puts %[INSERT INTO users (email, name, customer_id) VALUES ("#{email}", "#{first_name} #{last_name}", #{customer_id});]
end

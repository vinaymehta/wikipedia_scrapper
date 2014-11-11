require 'open-uri'
require 'rubygems/package'
require 'zlib'
require "i18n"

desc "Import Wiki Count for languages"
task :redis_wiki_scrapper, [:arg1, :arg2] => [:environment] do |t, args|

	start_date = args[:arg1].to_date
	end_date = args[:arg2].to_date
	(start_date..end_date).each do |date|
		parent_url = "https://dumps.wikimedia.org/other/pagecounts-raw/#{date.year}/#{date.year}-#{date.month.to_s.rjust(2,'0')}/"
		doc = Nokogiri::HTML(open(parent_url))
		links = doc.css('ul li a')
		links.each do |link|
			val = date.to_time.strftime('%Y%m%d')
			href = link.attributes['href'].value
			if href.include? val
				url = parent_url + href
				redis_download_and_extract_file(url, href, date)
			end
		end
	end
end

def redis_download_and_extract_file(url, file_name, date)
	file_path = "#{Rails.public_path}/#{file_name.split('.')[0]}.txt"
	open(file_path, 'wb') do |local_file|
    open(url) do |remote_file|
      local_file.write(Zlib::GzipReader.new(remote_file).read)
    end
  end
  redis_read_and_save_data(file_path, date)
end

def redis_read_and_save_data(file_path, date)
	csv_path = Rails.root.join("wiki_urls.csv")
	if File.exists?(file_path)
		CSV.open(csv_path, 'r:bom|utf-8', headers: false, col_sep: "\t", quote_char: "\"").each_with_index do |line, index|
			line = line[0].split(',') if line.length <= 1
			title = I18n.transliterate(line[1])
			wiki_id = line[0]
			count_hash = $redis.hgetall("#{wiki_id}:#{date.to_time.strftime('%d/%m/%Y')}")
			for i in 2..line.length-1
				unless line[i].blank?
					lang = line[i].split('http://')[1].split('.')[0].downcase unless line[i].split('http://')[1].blank?
					unless lang.blank?
						count_hash[lang] = count_hash[lang].blank? ? 0 : count_hash[lang].to_i
						begin
							link_url =  URI.encode(line[i])
							if link_url.include? 'curid='
								search_title = "curid=#{link_url.split('?curid=').last}"
							else
								search_title = " #{I18n.transliterate(link_url.split('wiki/').last)} "
							end
							arr = open(file_path, 'r:iso8859-1').grep(/#{search_title}/)
							arr.each do |line_val|
								if line_val.include? "curid="
									count_hash[lang]+=line_val.split(' ')[2].to_i
								else
									if line_val.split(' ').first == lang
										count_hash[lang]+=line_val.split(' ')[2].to_i
									end
								end
							end
						rescue Exception => e
							puts e
						end
					end
				end
			end
			$redis.mapped_hmset("#{wiki_id}:#{date.to_time.strftime('%d/%m/%Y')}", count_hash)
		end 
		File.delete file_path
	else
		puts "no"
	end
end

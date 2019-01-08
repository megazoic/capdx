#this app will pull data from a folder containing MS Excel files holding OR DEQ
#air pollution data in various sheets. Interested in sheets 1 Facility Info,
#2 Emission Units & Activities and 3 Material Balance

require 'roo'
require 'pstore'
require 'basecustom'
require 'logger'

class DataExtractor
    @@logger = Logger.new File.new('log/extract.log', 'w')
    @@logger.formatter = proc do |severity, datetime, progname, msg|
      "#{severity}: #{msg}\n"
    end
    #external DB files
    @@co_details_store = PStore.new("./db_files/co_details.pstore")
    @@material_chem_data_store = PStore.new("./db_files/material_chem_data.pstore")
    @@emission_chem_data_store = PStore.new("./db_files/emission_chem_data.pstore")
    @@material_desc_store = PStore.new("./db_files/material_desc.pstore")
    @@emission_desc_store = PStore.new("./db_files/emission_desc.pstore")
    @@material_agg_data_store = PStore.new("./db_files/material_agg_data.pstore")
    @@emission_agg_data_store = PStore.new("./db_files/emission_agg_data.pstore")
    #holds unique set of units
    @@desc_agg_units_store = PStore.new("./db_files/desc_agg_units.pstore")
    
    #lookup table for unique_row_ID => row_no, sheet, co_source_no, file_name
    @@row_lookup_store = PStore.new("./db_files/row_lookup.pstore")

    #Excel col headings
    @@baseABC = BaseCustom.new("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

    #misc
    WORKINGXLDIR = './data_files/'
    SHEET = Hash["MAT",'3. Material Balance',"EMI",'2. Emission Units & Activities',"CO",'1. Facility Information']
    
    @@cas_pattern = 3 #default col pattern for picking up chem specific data
    @@cas_formula = '' #holds formula used to calculate cas data and this formula is held in desc_agg_units_store
    
    @@co_source_no = ''
    @@current_row_ID = '' #is unique but this variable is reset every row
    @@current_cas_col_ID = Hash["EMI", "000000", "MAT", "000000"] #holds unique ids for chem specific data in chem_data_store
    @@unique_row_ID_array = []
    @@data_cas_anchor = [] #holds upper left corner of congituous range of data cells row number
    #and the cas_row and cas_col anchors so, [data_anchor, cas_row_anchor, cas_col_anchor]
    #start with cas anchor row at 15
    @@data_cas_anchor[1] = 15
    @@bottom_row = '' #need to get rid of this
    #build hash to hold row numbers that have data in them
    #*_rows is array [[first_data_row no, unique_row_ID], [second_data_row, unique_row_ID], ...]
    @@data_present = Hash["EMI", false, "EMI_rows", [], "MAT", false, "MAT_rows", [], "EMI_unit_key", '', "MAT_unit_key", '']

    #_______________________data processing functions____________
    def self.enumerateFiles
        #used to walk through files hand off Roo sheets for processing
        files_to_check = Dir::glob(WORKINGXLDIR + "*.xlsx")
        file_count = 0
        file_name = ""
        files_to_check.each do |file|
          #if file_count == 215
          #    break
          #elsif file_count < 213
          #    file_count = file_count + 1
          #    next
          #end
          fn = /\/([^\/]+)\.xlsx/.match(file)
          if fn.nil?
              @@logger.error "could not match filename"
              file_name = "NA"
          else
              file_name = fn[1]
          end
          puts "processing file #{file_count}, #{file_name}"
          begin
              roo_file = Roo::Spreadsheet.open(file)
          rescue ArgumentError => e
              @@logger.error "there was an error with file #{file_name}"
              @@logger.error e.backtrace
              @@logger.error "here's the obj msg"
              @@logger.error e.message
              return 0
          end
          setCoInfo(roo_file, file_name)
          #test for data present in file, MUST BUILD EMI FIRST
         ["EMI", "MAT"].each do |sheet_key|
            #check if there is data in this sheet, if so, get data row and cas cell anchors
            success = findCasDataAnchor(roo_file, sheet_key, file_name)
            if success == 1
              #next step is to build the array of rows that contain data
              setDataRowArray(roo_file, sheet_key)
              #add that we have data for this sheet to @@co_details_store
              @@co_details_store.transaction do
                cf = @@cas_formula
                if cf == ''
                  cf = 'NA'
                end
                @@co_details_store[@@co_source_no][sheet_key] = [@@data_present[sheet_key],cf]
              end
              
              if !@@data_present[sheet_key]
                #none, try next sheet
                @@logger.info "#{@@co_source_no}: not able to find any data in #{sheet_key}_rows"
                next
              end
              #_____________________________setDataColDescriptors__________________________
              #need to store the data column descriptors then add descriptors to the *_desc_store
              success = setDescriptorsAndData(roo_file, sheet_key, file_name)
              if success == 1
                #finally, store the chem-specific data
                #first check to see if there is any cas data @@data_cas_anchor[1] default is 15 so check [2]
                if @@data_cas_anchor[2].nil?
                  next
                end
                getChemSpecificData(roo_file, sheet_key, file_name)
              else
                puts "setDescriptorsAndData no joy!"
              end
              reset("btwn_sheets")
            end#close if success == 1
          end
          reset("eof")
          file_count = file_count + 1
        end
    end
    def self.setDescriptorsAndData(roo_file, sheet_key, file_name)
      #need to walk through the range of rows and pull out the data descriptors, aggregate data
      tmp_units_array = []
      #emission sheet has 3 cols of descriptors and 8 cols of aggregate data = 11 tot
      #materials have 4 and 10 = 14 tot
      end_col = sheet_key == "EMI" ? 11 : 14
      #will separate out the descriptors from aggregate data
      desc_col_end = sheet_key == "EMI" ? 3 : 4
      #first get the units for each of the cols associated with desc and agg
      begin_row = @@data_cas_anchor[0]
      tmp_units_key = ''
      (1..3).each do |row_adj|
        cell = roo_file.sheet(SHEET[sheet_key]).cell(begin_row - row_adj, 1).to_s
        if /Unit/.match(cell)
          #have our unit row, check to see if cas_anchor has been set
          if !@@data_cas_anchor[2].nil?
            #use existing cas_col as end column for picking up units
            end_col = @@data_cas_anchor[2] - 1
          end
          #walk the cols until greater than end_col and check to see if empty
          (1..end_col).each do |col|
            tmp_unit = roo_file.sheet(SHEET[sheet_key]).cell(begin_row - row_adj, col).strip
            #clean up tmp_units
            tmp_unit.gsub!(/\s+/,' ')
            #clean up tmp_units array
            if /<[^>]+>/.match(tmp_unit)
              #need to remove html code
              tags = tmp_unit.scan(/<[^>]+>/)
              tags.each do |tag|
                tmp_unit.gsub!(tag, ' ')
                tmp_unit.gsub!(/\s+/, ' ')
              end
            end
            tmp_units_array << tmp_unit
            #determine where descriptors end and aggregate data begin, only have to adj with EMI sheet
            if sheet_key == 'EMI'
              if col == 3
                if /SCC/.match(tmp_unit)
                  desc_col_end = 3
                else
                  desc_col_end = 2
                end
              end
            end
          end
          #check to see if already have this unit set
          @@desc_agg_units_store.transaction do
            @@desc_agg_units_store.roots.each do |key|
              #the array at element 0 is all of the col headers
              if @@desc_agg_units_store[key][0].eql?(tmp_units_array)
                #there is already a key with this set of units
                tmp_units_key = key
                break
              end
            end#close testing keys and unit sets
            if tmp_units_key == ''
              #didn't find this units set, need to generate new and add these to the store
              key_not_unique = true
              while key_not_unique
                rng = Random.new
                tmp_units_key = rng.rand(10000).to_s
                #look for existing key if not present, add this to the store
                if !@@desc_agg_units_store.root?(tmp_units_key)
                  key_not_unique = false
                  # second element of this array references which col is the end of the descriptors
                  @@desc_agg_units_store[tmp_units_key] = [tmp_units_array, desc_col_end]
                  @@logger.info "*******#{@@co_source_no}:#{sheet_key} have tmp_units**********\n#{tmp_units_key}\n#{tmp_units_array}"
                end
              end#end while
            end#already have this set of units keep tmp_units_key to add to the storage of data rows
          end#close store transaction
          @@data_present["#{sheet_key}_unit_key"] = tmp_units_key
          break
        end#close if Unit match
        if row_adj == 3
          #didn't find a matching cell for Emission Units Activity ID report error
          @@logger.error "#{@@co_source_no}:#{sheet_key} not able to find units anchor"
          return 0
        end
      end#close (1..3) loop acquisition of units
      #now get the desc and agg data in the data range, already know we have data present
      count = 0
      @@data_present[sheet_key + '_rows'].each do |row_array|
        #need to walk through each row from upper left hand 
        tmp_descriptor_array = []
        tmp_agg_array = []
          (1..end_col).each do |col|
              tmp_data = roo_file.sheet(SHEET[sheet_key]).cell(row_array[0], col)
              if tmp_data.nil?
                  tmp_data = 'NA'
              end
              if col > desc_col_end
                #must be picking up aggregate data
                tmp_agg_array << tmp_data
              else
                #picking up unit descriptor info
                tmp_descriptor_array << tmp_data
              end
          end
          #generate a unique row id (primary key for the chem_specific/agg/desc_store tables)
          next_key =  @@unique_row_ID_array.last
          if next_key.nil?
            next_key = '00001'
          else
            next_key = next_key.strip.to_i + 1
          end
          @@current_row_ID = next_key.to_s.rjust(5,"0")
          if !@@unique_row_ID_array.include?(@@current_row_ID)
              @@unique_row_ID_array << @@current_row_ID
              #store this as well as the current sheet row in the row_lookup store
              #make room for any chem_specific_data_IDs in the empty array
              @@row_lookup_store.transaction do
                @@row_lookup_store[@@current_row_ID] = [row_array[0], [nil], sheet_key, @@co_source_no, file_name]
              end
          end
          #add the current_row_ID to the @@data_present
          @@data_present[sheet_key + '_rows'][count] << @@current_row_ID
          #add the sheet_key_unit_key
          tmp_descriptor_array << @@data_present["#{sheet_key}_unit_key"]
          tmp_agg_array << @@data_present["#{sheet_key}_unit_key"]
          if sheet_key == "EMI"
            @@emission_desc_store.transaction do
                @@emission_desc_store[@@current_row_ID] = tmp_descriptor_array
            end
            @@emission_agg_data_store.transaction do
              @@emission_agg_data_store[@@current_row_ID] = tmp_agg_array
            end
          else
            @@material_desc_store.transaction do
                @@material_desc_store[@@current_row_ID] = tmp_descriptor_array
            end
            @@material_agg_data_store.transaction do
              @@material_agg_data_store[@@current_row_ID] = tmp_agg_array
            end
          end
          count += 1
          #success
      end #close data_present each (walking through all rows)
      return 1
    end
    def self.setCoInfo(roo_file, file_name)
        #pull out the company information
            start_row = 12
            begin_data = roo_file.sheet(SHEET["CO"]).cell(start_row, 1).to_s
            if /Facility Information/.match(begin_data)
              #facility_array will have [file_name, co_name, co_st_addr,
              #co_city_addr, co_zip, EMI-data_present, MAT-data_present]
              facility_array = [file_name]
              (1..4).each do |row_inc|
                  facility_array << roo_file.sheet(SHEET["CO"]).cell(start_row + row_inc,2)
              end
              #need to check company source number since sometimes the address fields take up more rows
              source_row_inc = 5
              while true
                  source_check = roo_file.sheet(SHEET["CO"]).cell(start_row + source_row_inc,1)
                  if /Source/.match(source_check)
                    #company source code should be cell to the right of this except one file has format dd/dd/dddd
                    co_source_no = roo_file.sheet(SHEET["CO"]).cell(start_row + source_row_inc,2)
                    if co_source_no.is_a?(Date)
                      co_source_no = co_source_no.strftime("%m/%d/%Y")
                    elsif co_source_no.is_a?(Integer)
                      #22-0143 entered 220143
                      tmp = co_source_no.to_s
                      co_source_no = "#{tmp[0,2]}-#{tmp[2,6]}"
                    elsif co_source_no == "" || co_source_no.nil?
                      co_source_no = file_name[0,7]
                    end
                    m = /(\d+)[-\/](\d+)\/*(\d*)/.match(co_source_no)
                    if m[3] == "" #file should be 02-2125 but entered wrong in sheet (02/01/2125)
                      @@co_source_no = co_source_no
                    else
                      @@co_source_no = "#{m[1]}-#{m[3]}"
                      @@logger.info "#{file_name}: co_source_no had date format converted to: #{@@co_source_no}"
                    end
                    break
                  else
                      #going to look from rows 17 to 20 for the word Source
                      if source_row_inc > 8
                          #no joy
                          @@logger.error "no co_source_no could be found! Using begining of file name"
                          m = /([\d-]+)/.match(file_name)
                          if m[1].nil?
                            #one file has format dd/dd/dddd
                            @@co_source_no = file_name[0,10]
                          else
                            @@co_source_no = m[1]
                          end
                          break
                      end
                      source_row_inc = source_row_inc + 1
                  end
              end
              @@co_details_store.transaction do
                  if @@co_details_store.root?(@@co_source_no)
                      #we already have this data
                      @@logger.info "this company #{@@co_source_no} has already been entered"
                  else
                      @@logger.info "adding #{@@co_source_no} to the store for file #{file_name}"
                      @@co_details_store[@@co_source_no] = Hash["addr",facility_array]
                  end
              end
              @@logger.info "adding facility info for #{file_name}"
            else
                @@logger.error "there was an error finding facility info for #{file_name}"
            end
    end

    def self.getChemSpecificData(roo_file, sheet_key, file_name)
        #pull out the cas-specific data
        sheet = SHEET[sheet_key]
        begin
            noCols = roo_file.sheet(sheet).last_column
        rescue ArgumentError => e
            @@logger.error "there was an error with file #{fileName}"
            @@logger.error e.backtrace
            @@logger.error "here's the obj msg"
            @@logger.error e.message
            return 0
        end
        subcount = 0
        skip_inc_subcount = false
        max_col_adjust = 0
        #check pattern of cas-codes, expect one every 3 cols
        (0..noCols).each do |col_count|
          #skip desc and agg data columns
          if col_count < (@@data_cas_anchor[2] + 1)
            next
          end
          #cas_code array holds code and desc, third cell used to differentiate between
          #group description col and chem with missing cas code (only a common name)
          cas_code_array = []
          if @@cas_pattern == 3
            if subcount%3 == 0
              tmp = roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1], col_count)
              if tmp.is_a?(String)
                tmp.strip
              end
              cas_code_array[0] = tmp
              cas_code_array[1] = roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1] + 1, col_count)
              cas_code_array[2] = roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1] + 2, col_count)
              case self.validate(cas_code_array)
              when "orphan"
                #represents a correct col without a cas but does have a chem description
                found_CAS = false
                #check for any data at this chem type
                chem_by_type_data_array = getChemDataByType(roo_file, sheet_key, col_count)
                if chem_by_type_data_array != 0
                  setChemSpecificData(roo_file, sheet_key, col_count, cas_code_array, chem_by_type_data_array, found_CAS)
                end
                skip_inc_subcount = false
              when "cas"
                found_CAS = true
                #check for any data at this chem type
                chem_by_type_data_array = getChemDataByType(roo_file, sheet_key, col_count)
                if chem_by_type_data_array != 0
                  setChemSpecificData(roo_file, sheet_key, col_count, cas_code_array, chem_by_type_data_array, found_CAS)
                end
                skip_inc_subcount = false
              else
                #must be a chemical group desc move to next col and look again
                skip_inc_subcount = true
              end
            end
            if skip_inc_subcount == false
              #need to reset max_col_adjust because back on track
              max_col_adjust = 0
              subcount = subcount + 1
            else
              #must skip a col somethings up but only allow 3 more adj cols
              if max_col_adjust > 3
                return 0
              else
                max_col_adjust = max_col_adjust + 1
              end
            end
          else#@@cas_pattern must not be 3 but 1: pull out every column as new CAS
            tmp = roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1], col_count)
            if tmp.is_a?(String)
              tmp.strip
            end
            cas_code_array[0] = tmp
            cas_code_array[1] = roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1] + 1, col_count)
            cas_code_array[2] = roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1] + 2, col_count)
            case self.validate(cas_code_array)
            when "orphan"
              found_CAS = false
              chem_by_type_data_array = getChemDataByType(roo_file, sheet_key, col_count)
              if chem_by_type_data_array != 0
                setChemSpecificData(roo_file, sheet_key, col_count, cas_code_array, chem_by_type_data_array, found_CAS)
              end
            when "cas"
              found_CAS = true
              chem_by_type_data_array = getChemDataByType(roo_file, sheet_key, col_count)
              if chem_by_type_data_array != 0
                setChemSpecificData(roo_file, sheet_key, col_count, cas_code_array, chem_by_type_data_array, found_CAS)
              end
            else
              @@logger.info "#{file_name}:#{sheet_key} col pattern was 1, cas_code_array not orphan or cas"
            end
          end#close if @@cas_pattern == 3
        end#closes (0..noCols) do |col_count|
     end

    #_______________________helper functions______________________
    def self.spiralSearch(colDim, rowDim)
      #stackoverflow.com/questions/398299/looping-in-a-spiral
      #searchs a rectangle grid in a spiral pattern
      sx = colDim/2
      sy= rowDim/2
      #cx and cy are initial position offsets
      cx = cy = 0
      direction = distance = 1
  
      yield(cx,cy)
      while(cx.abs <= sx || cy.abs <= sy)
        distance.times {cx += direction; yield(cx,cy) if(cx.abs <= sx && cy.abs <= sy);}
        distance.times {cy += direction; yield(cx,cy) if(cx.abs <= sx && cy.abs <= sy);}
        distance += 1
        direction *= -1
      end
    end
    def self.setChemSpecificData(roo_file, sheet_key, col_count, cas_code_array, chem_by_type_data_array, found_CAS)
      #there is data in this chemical type, need to pick up the cas code, name and 3 units
      #and write to store
      col_heading = convertNumColToExlCol(col_count)
      chem_by_type_hash = Hash.new
      if found_CAS
        chem_by_type_hash["CAS"] = cas_code_array[0]
      else
        chem_by_type_hash["CAS"] = "NA_#{cas_code_array[1][0,15]}"
      end
      chem_by_type_hash["CAS_name"] = cas_code_array[1].strip
      if @@cas_pattern == 3
        units = [cas_code_array[2]]
        units << roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1] + 2, col_count + 1)
        units << roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1] + 2, col_count + 2)
        #need to pull out the juicy bits
        unit_reg = /\((.+)\)/
        #the MAT sheet will have unitA %Weight Pollutant all units will be in para eg. (lbs)
        start = sheet_key == "EMI" ? 0 : 1
        (start..2).each do |count|
          m = unit_reg.match(units[count])
          if !m[1].nil?
            units[count] = m[1]
          end
        end
      else
        units = ["% weight"]
      end
      chem_by_type_hash["Unit_A"] = units[0]
      if @@cas_pattern == 3
        chem_by_type_hash["Unit_B"] = units[1]
        chem_by_type_hash["Unit_C"] = units[2]
      end
      #build array to write to store
      chem_by_type_data_array.each do |data_row|
        #data_row[0] array has unitA, B, C of data, data_row[1] has unique_row_ID
        #add unique_for_ID and Excel col header
        out_array = [data_row[1], col_heading]
        #add CAS info
        out_array << chem_by_type_hash["CAS"]
        out_array << chem_by_type_hash["CAS_name"]
        out_array << chem_by_type_hash["Unit_A"]
        out_array << data_row[0][0]
        if @@cas_pattern == 3
          out_array << chem_by_type_hash["Unit_B"]
          out_array << data_row[0][1]
          out_array << chem_by_type_hash["Unit_C"]
          out_array << data_row[0][2]
        end
        if sheet_key == "EMI"
          @@emission_chem_data_store.transaction do
            store_key = @@current_cas_col_ID["EMI"]
            next_key = (store_key.to_i + 1).to_s.rjust(6,"0")
            @@emission_chem_data_store[store_key] = out_array
            @@current_cas_col_ID["EMI"] = next_key
            #update the row_lookup_store
            @@row_lookup_store.transaction do
              row_array = @@row_lookup_store[@@current_row_ID]
              if row_array[1][0].nil?
                row_array[1] = [store_key]
              else
                row_array[1] << store_key
              end
              @@row_lookup_store[@@current_row_ID] = row_array
            end
          end
        else
          @@material_chem_data_store.transaction do
            store_key = @@current_cas_col_ID["MAT"]
            next_key = (store_key.to_i + 1).to_s.rjust(6,"0")
            @@material_chem_data_store[store_key] = out_array
            @@current_cas_col_ID["MAT"] = next_key
            #update the row_lookup_store
            @@row_lookup_store.transaction do
              row_array = @@row_lookup_store[@@current_row_ID]
              if row_array[1][0].nil?
                row_array[1] = [store_key]
              else
                row_array[1] << store_key
              end
              @@row_lookup_store[@@current_row_ID] = row_array
            end
          end
        end
      end
    end
    def self.getChemDataByType(roo_file, sheet_key, data_col)
      #build 2D array to return data [[row_data, row_unique_ID]]
      data_out = []
      data_to_return = false
      @@data_present[sheet_key + "_rows"].each do |data_row_array|
        #collect all of the data in the row associated with this CAS col (even if only one row)
        row_data = []
        #check to see how many columns of data are present (some files only have one, others 3)
        (0..2).each do |col_adder|
          cell = roo_file.sheet(SHEET[sheet_key]).cell(data_row_array[0], data_col + col_adder).to_s
          if cell == ""
            row_data << nil
          else
            row_data << cell
          end
        end
        if row_data.compact.empty?
          #no data on this row for this CAS, move to next row
        else
          data_to_return = true
          #add unique_row_ID
          data_out << [row_data, data_row_array[1]]
        end
      end#close @@data_present enumerator
      if data_to_return == true
        return data_out
      else
        return 0
      end
    end
    def self.convertNumColToExlCol(num_col)
        #this method takes a numeric col reference and returns an excel col label like ZZ after ZY
        if num_col < 27
            dec = num_col - 1
            exl_col = @@baseABC.base(dec)
        elsif num_col > 26 && num_col < 677
            dec = num_col - 1
            exl_col = @@baseABC.base(dec)
            char_to_change = exl_col[0].ord
            exl_col[0] = (char_to_change -1).chr
        elsif num_col > 676 && num_col < 703
            dec = num_col - 27
            exl_col = @@baseABC.base(dec)
        else
            dec = num_col - 27
            exl_col = @@baseABC.base(dec)
            char_to_change = exl_col[0].ord
            exl_col[0] = (char_to_change -1).chr
        end
        return exl_col
    end
    def self.validate(cas_code_array)
      #return true when not valid
      cas_reg = /^[#\d-]+$/
      desc_reg = /EF/
      #if cas_code[0].nil? || !cas_code[0].is_a?(String) || !cas_reg.match(cas_code[0])
      if cas_code_array[0] == ""
          #missing cas_code but desc present?
          if desc_reg.match(cas_code_array[2])
              #in right place but no cas_code
              return "orphan"
          else
              return 0
          end
      elsif cas_reg.match(cas_code_array[0])
          return "cas"
      else
          return 0
      end
    end
    def self.reset(reset_type)
      case reset_type
      when "btwn_sheets"
        @@data_cas_anchor = []
        @@data_cas_anchor[1] = 15
        @@cas_formula = ''
        @@cas_pattern = 3
      when "eof"
        @@data_cas_anchor = []
        @@data_cas_anchor[1] = 15
        @@cas_formula = ''
        @@cas_pattern = 3
        @@data_present = Hash["EMI", false, "EMI_rows", [], "MAT", false, "MAT_rows", [], "EMI_unit_key", '', "MAT_unit_key", '']
      else
      end
    end
    def self.findCasDataAnchor(roo_file, sheet_key, file_name)
      #_____________________________look for data row anchor__________________________
      #look for the cell containing data that is at upper left of range
      data_found = false
      if sheet_key == 'EMI'
        (10..21).each do |row|
          cell = roo_file.sheet(SHEET[sheet_key]).cell(row, 1).to_s
          m = /Emis/.match(cell)
          if m
            #test to see if there is a cell below with the same Emission chars in string
            cell = roo_file.sheet(SHEET[sheet_key]).cell(row + 1, 1).to_s
            m = /Emis/.match(cell)
            if m
              #see if "Example calculation is in next cell"
              cell = roo_file.sheet(SHEET[sheet_key]).cell(row + 2, 1).to_s
              m = /Example/.match(cell)
              if m
                emi_anchor_row = row + 3
              else
                emi_anchor_row = row + 2
              end
            elsif cell == ""
              @@logger.info "#{@@co_source_no}: setting Anchor row for EMI\nrow below first emission match has nothing in it"
              emi_anchor_row = row + 1
            else #cell must have data
              emi_anchor_row = row + 1
            end
            @@data_cas_anchor[0] = emi_anchor_row
            data_found = true
            break
          end
        end
        if data_found == false
          @@logger.error "#{@@co_source_no}: setting Anchor row for EMI\ncouldn't find emission match"
        end
      else #look in sheet 'MAT'
        spiralSearch(3,11) { |col_shift,row_shift|
          col = 2 + col_shift
          row = 15 + row_shift
          cell = roo_file.sheet(SHEET[sheet_key]).cell(row, col).to_s
          m = /Prod/.match(cell)
          if m
            #data row is two rows below this cell
            @@data_cas_anchor[0] = (row + 2)
            data_found = true
            break
          end
        }
        if data_found == false
          @@logger.error "#{@@co_source_no} not able to find data row anchor in MAT"
        end
      end
      if data_found == false
        #no point in continuing with this sheet
        return 0
      end
      #_____________________________look for cas anchor__________________________
      #the cells with CAS title and Total Emissions are found in different columns
      start_col = sheet_key == "EMI" ? 12 : 14
      cell = roo_file.sheet(SHEET[sheet_key]).cell(@@data_cas_anchor[1], start_col).to_s
      if /CAS/.match(cell)
        #put this row col into @@data_cas_anchor (already contains data_anchor (row) and cas row)
        @@data_cas_anchor[2] = start_col
        #set the col pattern and cas formula
        setColPatternCasFormula(roo_file, sheet_key, file_name, @@data_cas_anchor[1], start_col)
        #got something from this sheet move on
        return 1
      else
        #need to try a spiral search pattern
        missing_cas_string = []
        spiralSearch(7,5) { |col_shift,row_shift|
          col = start_col + col_shift
          row = @@data_cas_anchor[1] + row_shift
          cell = roo_file.sheet(SHEET[sheet_key]).cell(row, col).to_s.strip
          if /CAS/.match(cell)
            #set both row (even though it already exists here) and col
            @@data_cas_anchor[1,2] = row, col
            @@logger.info "#{@@co_source_no}:#{sheet_key} has data_row: #{@@data_cas_anchor[0]}, cas_row: #{@@data_cas_anchor[1]}, cas_col: #{@@data_cas_anchor[2]}"
            #set the col pattern and cas formula
            setColPatternCasFormula(roo_file, sheet_key, file_name, row, col)
            #got something from this sheet move on
            return 1
          end
        }
        #if we're here, we couldn't find a cas anchor cell but we did find a data anchor, ok to continue
        @@logger.info "#{@@co_source_no}:#{sheet_key} couldn't find a cas row but data row: #{@@data_cas_anchor[0]}"
        return 1
      end
    end
    def self.setColPatternCasFormula(roo_file, sheet_key, file_name, row, col)
      #Test adj cells (same row, cols to right) for 3 col or 1 col pattern (expect cas number adjacent to right)
      cas_reg = /^[\d-]+$/
      cells_with_cas = []
      (1..2).each do |col_addr|
        cells_with_cas << roo_file.sheet(SHEET[sheet_key]).cell(row, (col + col_addr)).to_s
      end
      m1 = cas_reg.match(cells_with_cas[0])
      m2 = cas_reg.match(cells_with_cas[1])
      if m1.nil?
        #expected this to be true raise error
        @@logger.error "#{file_name}:#{sheet_key} cannot find cas number adj to CAS anchor"
      end
      if m2.nil?
        @@cas_pattern = 3 #this is a 3 col pattern (default)
        #need to look up formula, step through rows on 'CAS' col looking for 'Emis' then 'Pollu'
        form_reg = /(Emis|Pollu)/i
        cells_with_cas = []
        (1..2).each do |row_addr|
          cells_with_cas << roo_file.sheet(SHEET[sheet_key]).cell((row + row_addr), col).to_s
        end
        m1 = form_reg.match(cells_with_cas[0])
        m2 = form_reg.match(cells_with_cas[1])
        if !m1.nil? && !m2.nil?
          #look 3 rows below CAS string
          formula = roo_file.sheet(SHEET[sheet_key]).cell((row + 3), col).to_s.strip
          if formula != ''
            @@cas_formula = formula
          else
            #try one more row
            formula = roo_file.sheet(SHEET[sheet_key]).cell((row + 4), col).to_s.strip
            if formula != ''
              @@cas_formula = formula
            else
              @@logger.error "#{@@co_source_no}:#{sheet_key} has no formula"
            end
          end
        else
          if m2.nil?
            formula = roo_file.sheet(SHEET[sheet_key]).cell((row + 2), col).to_s.strip
            if formula != ''
              @@cas_formula = formula
            else
              #try one more row
              formula = roo_file.sheet(SHEET[sheet_key]).cell((row + 3), col).to_s.strip
              if formula != ''
                @@cas_formula = formula
              else
                @@logger.error "#{@@co_source_no}:#{sheet_key} has no formula"
              end
            end
          else
            @@logger.error "#{@@co_source_no}:#{sheet_key} m1 was nil"
          end
        end
      else
        @@cas_pattern = 1
        @@logger.info "#{file_name}:#{sheet_key} cas pattern is single column"
      end
    end
    def self.setDataRowArray(roo_file, sheet_key)
      @@data_present[sheet_key] = false
      max_rows = roo_file.sheet(SHEET[sheet_key]).last_row
      anchor_row = @@data_cas_anchor[0]
      #get some Emission Units or Activity IDs
      leeway_count = 0
      collecting_data = false
      data_rows = 0
      (anchor_row..max_rows).each do |target_row|
        cell = roo_file.sheet(SHEET[sheet_key]).cell(target_row, 1).to_s
        if cell.nil? || cell == "" || /Example/.match(cell)
          if leeway_count > 3
            if collecting_data
              @@logger.info "#{@@co_source_no}: #{sheet_key} has #{data_rows} rows of data"
            end
            break
          else
            leeway_count += 1
          end
        else # have a row with data
          if !collecting_data
            @@data_present[sheet_key] = true
            collecting_data = true
          end
          @@data_present[sheet_key + '_rows'] << [target_row]
          data_rows += 1
        end
        #just in case
        if target_row == max_rows
          @@logger.info "#{@@co_source_no}: #{sheet_key} has #{data_rows} rows of data"
        end
      end#close anchor_row..max_rows
    end
end
DataExtractor.enumerateFiles

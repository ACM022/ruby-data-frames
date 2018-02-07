require 'csv'

# DataFrames in Ruby
class DataFrame
  # Takes data either as an array of hashes (each with the same keys),
  # or a hash of arrays (all of the same length and data type)
  def initialize(data)
    check_input_data(data)
    if data.class == Array
      # Set the columns of the DataFrame to be the unique keys contained throughout the data
      # Throw an error if column names are present in some row-hashes, but not others
      @df = data.each_with_object({}) {|row, dhash| row.keys.each {|field| dhash[field] = [] if !dhash.keys.include?(field)}}
      data.each do |row|
        @df.keys.each do |col|
          if row.keys.include?(col)
            @df[col].push(row[col])
          else
            # Ensure all of the data-array hash-rows have the same keys (columns)
            raise Exception.new("Certain input row-hashes do not contain the following column: #{col}")
          end
        end
      end
    elsif data.class == Hash
      @df = data
    end
    @dtypes = @df.keys.each_with_object({}) do |col, hash|
      hash[col] = find_data_type(col)
    end
    return ''
  end

  # Checks to ensure the consituent parts of the input data are of the correct class
  # Currently only allowing column headers to consist of the following classes: String, Fixnum, Float, Date, Time, or DateTime
  def check_input_data(data)
    if data.class == Array
      data.each do |row|
        # Ensure the data-array only contains hash values
        if row.class != Hash
          raise Exception.new('Input data-array contains non-hash values.')
        else
          # Ensure all column headers are strings, fixnums, floats, dates, times, or datetimes
          row.keys.each {|col| check_col_name(col)}
        end
      end
    elsif data.class == Hash
      col_lengths = []
      data.keys.each do |col|
        # Ensure all column names are strings, fixnums, floats, dates, times, or datetimes
        check_col_name(col)
        # Ensure all of the data-hash values are arrays
        if data[col].class != Array
          raise Exception.new('Input data-hash contains non-array values.')
        else
          col_lengths.push(data[col].length)
        end
      end
      # Ensure that all of the data-hash array-columns are of the same length
      if col_lengths.uniq.length != 1
        raise Exception.new('Data-hash array-columns are not all of the same length.')
      end
    else
      raise Exception.new("Object to convert to DataFrame must either be an array of row-hashes, or a hash of array-columns (#{data.class} object provided).")
    end
  end

  # Check column name to make sure it is of a valid data type: String, Fixnum, Float, Date, Time, or DateTime
  def check_col_name(col_name)
    valid_header_classes = [String, Fixnum, Float, Date, Time, DateTime]
    if !valid_header_classes.include?(col_name.class)
      raise Exception.new("Invalid header class detected. Headers can only be from the following classes: #{valid_header_classes.join(', ')}.")
    end
  end

  # Check to see if the name of the column to be added is already a column name in the DataFrame
  def check_if_col_already_exists(col_name)
    if @df.keys.include?(col_name)
      raise Exception.new("Column named '#{col_name}' already exists in the DataFrame.")
    end
  end

  # Check if the specified column name exists in the DataFrame
  def check_if_col_exists(col_name)
    if !@df.keys.include?(col_name)
      raise Exception.new("Column named '#{col_name}' does not exist in the DataFrame.")
    end
  end

  # Check if the specified input is either true or false
  def check_true_false(input)
    if ![true, false].include?(input)
      raise Exception.new("Specified input must be true or false. '#{input}' value was provided.")
    end
  end

  # Check to see if all of the values in the specified column are of a homogeneous data type.
  # If they are, return the data type; if not, throw an exception
  # Not counting nil, however--NilClass plus another data type will not cause an error
  # Also, if all values are nil, then label the column as a NilClass column
  def find_data_type(col)
    dtypes = @df[col].each_with_object([]) {|val, arr| arr << val.class if !arr.include?(val.class) && val.class != NilClass}
    if dtypes.length == 1
      dtypes[0]
    elsif dtypes.length > 1
      raise Exception.new("The '#{col}' column contains multiple non-nil class types: '#{dtypes.join(', ')}'")
    else
      NilClass
    end
  end

  # Display the data
  def disp(limit: nil)
    limit = @df[@df.keys[0]].length if limit == nil
    if limit.class != Fixnum
      raise Exception.new('Limit specified must be of class Fixnum.')
    end
    # Get the maximum character width in each column (including header in search)
    col_widths = @df.keys.each_with_object({}) {|col_name, hash| hash[col_name] = [col_name.to_s.length]}
    @df.keys.each {|col| @df[col].each {|val| col_widths[col].push(val.to_s.length)}}
    # Assign default width amounts corresponding to the maximum number of characters in each column with a little extra space between columns
    width_max = col_widths.keys.each_with_object({}) {|col, hash| hash[col] = (col_widths[col].max + 5)}
    # Print the data in the DataFrame (PostgreSQL terminal output style)
    # Create and print header row
    header = ''
    @df.keys.each_with_index do |col, i|
      header += col.to_s
      header += ' ' * (width_max[col] - col.to_s.length) if i != @df.keys.length - 1
      header.gsub!(/     $/, '  |  ') if i != @df.keys.length - 1
    end
    puts header
    # Create and print line separating header from table data
    line = ''
    header.each_char {|char| char != '|' ? line += '-' : line += '+'}
    puts line
    # Print table data
    for row_index in 0...limit
      row = ''
      @df.keys.each_with_index do |col, i|
        row += @df[col][row_index].to_s
        row += ' ' * (width_max[col] - @df[col][row_index].to_s.length) if i != @df.keys.length - 1
        row.gsub!(/     $/, '  |  ') if i != @df.keys.length - 1
      end
      puts row
    end
    puts ''
    return '=' * 100
  end

  # Returns the number of rows in the DataFrane
  def nrows
    @df[@df.keys[0]].length
  end

  # Returns the number of columns in the DataFrame
  def ncols
    @df.keys.length
  end

  # Returns the dimensions of the DataFrame in an array, where the first value is the number of rows and the second value is the number of columns
  def shape
    [@df[@df.keys[0]].length, @df.keys.length]
  end

  # Returns the names of the columns in the DataFrame in an array
  def columns
    @df.keys
  end

  # Returns a table listing the data types of each column in the DataFrame
  def dtypes(print_types: true)
    check_true_false(print_types)
    max_width = @df.keys.map {|col_name| col_name.length}.max + 5
    if print_types
      puts '-' * 50
      @df.keys.each do |col_name|
        puts col_name + ' ' * (max_width - col_name.length) + @dtypes[col_name].to_s
      end
      puts '-' * 50
    end
    @dtypes
  end

  # Returns the DataFrame as an array of row-hashes
  def to_a
    data_arr = []
    for i in 0...@df[@df.keys[0]].length
      data_arr << @df.keys.each_with_object({}) {|col, rhash| rhash[col] = @df[col][i]}
    end
    data_arr
  end

  # Returns the DataFrame as a hash of column-arrays
  def to_h
    @df
  end

  # Check to see whether the specified object is an array. If it is not, raise an exception.
  def check_array(object)
    if object.class != Array
      raise Exception.new('Specified object must be an array.')
    end
  end

  # Check to see if the contents of the specified array are all valid column names.
  def check_if_col_names(array)
    check_array(array)
    array.each do |col_name|
      if !@df.keys.include?(col_name)
        raise Exception.new("Column name '#{col_name.to_s}' not found in DataFrame.")
      end
    end
  end

  # Returns true if the string is a number, returns false if the string is ''
  # (want to convert this to nil), throws exception otherwise
  def is_number?(string)
    if string == ''
      false
    else
      true if Float(string) rescue raise Exception.new("Cannot convert string value '#{string}' in column to integer/float.")
    end
  end

  # Returns a DataFrame containing the columns specified in the array specified columns
  def select_columns(cols)
    check_array(cols)
    check_if_col_names(cols)
    DataFrame.new(cols.each_with_object({}) {|col, dhash| dhash[col] = @df[col]})
  end

  # Return the values in the specified column as an array
  def select(col)
    check_if_col_names([col])
    @df[col]
  end

  # Select the rows that are true in the input truth array
  # The truth array must be assembled separately using the DataFrame.select(column)
  # method and the Vanilla Ruby Boolean operators and array.map method.
  # # Example creating truth array using cdis DataFrame
  # (0...cdis.nrows).to_a.map {|i| cdis.select('srs_total_tscore')[i] != '' && cdis.select('mrn')[i] != ''}
  # --> Not exactly elegant, but it gets the job done!
  def select_rows(select_array)
    check_array(select_array)
    raise Exception.new('Input array contains non-Boolean (true/false) values.') if (select_array.uniq - [true, false]).length > 0
    new_df = @df.keys.each_with_object({}) {|col, dhash| dhash[col] = []}
    select_array.each_with_index do |select_row, i|
      if select_row
        @df.keys.each {|col| new_df[col].push(@df[col][i])}
      end
    end
    DataFrame.new(new_df)
  end

  # Add a new column to the DataFrame. Input column must be an array of the same length as the columns already in the DataFrame
  def add_column(col_name, col_data)
    # Check to make sure column name is a valid class
    check_col_name(col_name)
    # Check to see if a column in the DataFrame already has the specified name
    check_if_col_already_exists(col_name)
    # Check to make sure the length of the column to be added is the same as the columns already in the DataFrame
    check_array(col_data)
    if @df[@df.keys[0]].length != col_data.length
      raise Exception.new("The length of the column to be added does not match the lengths of the other columns in the DataFrame.")
    end
    @df[col_name] = col_data
    # Get the data type of the new column that was added
    @dtypes[col_name] = find_data_type(col_name)
  end

  # Remove the specified column from the DataFrame. Does not throw an error if the specified column does not exist.
  def remove_column(col_name)
    @df.tap {|df| df.delete(col_name)}
    @dtypes.tap {|dtypes| dtypes.delete(col_name)}
    ''
  end

  # Change the name of the specified column to the new name specified
  def rename_column(old_name, new_name)
    check_if_col_exists(old_name)
    if old_name != new_name
      check_col_name(new_name)
      @df[new_name] = @df[old_name]
      @dtypes[new_name] = @dtypes[old_name]
      remove_column(old_name)
    end
  end

  # Convert the specified column(s) to the specified data type
  # Input could either be a single column name or an array of column names
  # Supported conversion classes: String, Fixnum, Float, Date, NilClass, and DateTime
  def convert(col, type)
    if ![String, Fixnum, Float, Date, DateTime, NilClass].include?(type)
      raise Exception.new('convert does not support the specified data type: ' + type.to_s)
    end
    if col.class == Array
      check_if_col_names(col)
      convert_column_type(col, type)
    else
      check_if_col_names([col])
      convert_column_type([col], type)
    end
  end

  # Do the 'dirty work' of converting the column data type
  # This assumes the classes can be converted between
  # If not, the normal conversion exception will be raised
  def convert_column_type(col_names, type)
    if type == String
      col_names.each {|col| @df[col] = @df[col].map {|v| v.to_s}}
      col_names.each {|col| @dtypes[col] = String}
    elsif type == Fixnum
      col_names.each {|col| @df[col] = @df[col].map {|v| is_number?(v) ? v.to_i : nil}}
      col_names.each {|col| @dtypes[col] = Fixnum}
    elsif type == Float
      col_names.each {|col| @df[col] = @df[col].map {|v| is_number?(v) ? v.to_f : nil}}
      col_names.each {|col| @dtypes[col] = Float}
    elsif type == Date
      col_names.each {|col| @df[col] = @df[col].map {|v| v.to_date}}
      col_names.each {|col| @dtypes[col] = Date}
    elsif type == DateTime
      col_names.each {|col| @df[col] = @df[col].map {|v| v.to_datetime}}
      col_names.each {|col| @dtypes[col] = DateTime}
    elsif type == NilClass
      col_names.each {|col| @df[col] = @df[col].map {|v| nil}}
      col_names.each {|col| @dtypes[col] = NilClass}
    end
  end

  # Insert the specified value in the specified column where the input truth array is true
  def change_values(col, val, select_array)
    check_if_col_names([col])
    # Check if the value to be set if of the same type as the data type of the column. If not, raise an exception.
    raise Exception.new("Data type of value to be set (#{val.class}) is not the same as the datatype of the column (#{col}: #{@dtypes[col]})") if val.class != @dtypes[col]
    # Ensure that the truth array is an array and it only contains true and false values
    check_array(select_array)
    raise Exception.new('Input array contains non-Boolean (true/false) values.') if (select_array.uniq - [true, false]).length > 0
    # If inputs are satisfactory, change the values in the specified column
    @df[col].each_index {|i| @df[col][i] = val if select_array[i]}
  end

  # Reduce all duplicated rows in the DataFrame to a single row
  def drop_duplicates(inplace: false)
    check_true_false(inplace)
    # Don't generate a new DataFrame because that will force it to run through all of the quality control Checks
    # Don't need to do this because this method is only applied to a DataFrame that is presumably already valid
    new_df = @df.keys.each_with_object({}) {|col_name, dhash| dhash[col_name] = []}
    uniq_rows = []
    for i in 0...@df[@df.keys[0]].length
      row_hash = @df.keys.each_with_object({}) {|col, rhash| rhash[col] = @df[col][i]}
      if !uniq_rows.include?(row_hash)
        uniq_rows << row_hash
        row_hash.keys.each {|col| new_df[col].push(row_hash[col])}
      end
    end
    if inplace
      @df = new_df
      ''
    else
      DataFrame.new(new_df)
    end
  end

  # Returns an array of hashes containing all unique combinations found among the specified fields in the DataFrame
  def get_uniq_combinations(cols, include_null: true)
    if include_null
      combos = (0...@df[@df.keys[0]].length).to_a.map do |i|
        cols.each_with_object({}) {|col, hash| hash[col] = @df[col][i]}
      end
    else
      # Only include combo if it has no '' and nil values in it
      combos = (0...@df[@df.keys[0]].length).to_a.each_with_object([]) do |i, arr|
        combo = cols.each_with_object({}) {|col, hash| hash[col] = @df[col][i]}
        arr << combo if !combo.values.include?('') && !combo.values.include?(nil)
      end
    end
    combos.uniq
  end

  # Returns a new DataFrame containing the rows where the values in the calling and extra specified
  # DataFrame are the same in the specified columns (that share the same name)
  # NOTE: all column names are converted to strings to make merge possible in cases where
  # there are columns with the same name in both DataFrames that are not used to merge on
  def merge(other_df, merge_cols, rsuffix: '_x', lsuffix: '_y')
    # Check inputs to make sure they are of correct classes/formats/variable names
    raise Exception.new("Object specified to merge to DataFrame must also be a DataFrame (#{other_df.class} object was specified).") if other_df.class != DataFrame
    check_array(merge_cols)
    merge_cols.each do |col|
      check_col_name(col)
      check_if_col_exists(col)
      raise Exception.new("Column '#{col}' not found in other DataFrame to merge to calling DataFrame.") if !other_df.columns.include?(col)
    end
    # Merge DataFrames
    new_df = @df.keys.each_with_object({}) do |col, hash|
      if !merge_cols.include?(col) && other_df.columns.include?(col)
        hash[col.to_s + rsuffix.to_s] = []
        hash[col.to_s + lsuffix.to_s] = []
      else
        hash[col.to_s] = []
      end
    end
    other_df.columns.each do |col|
      if !@df.keys.include?(col)
        new_df[col.to_s] = []
      elsif !merge_cols.include?(col)
        new_df[col.to_s + lsuffix] = []
      end
    end
    # Get all of the combinations in the merge columns that are shared between the two DataFrames
    common_combos = get_uniq_combinations(merge_cols, include_null: false) & other_df.get_uniq_combinations(merge_cols, include_null: false)
    # Cycle through each combo and enter all matches between the two DataFrames
    common_combos.each do |combo|
      # Cycle through each row in the calling DataFrame that contains the combo
      for i in 0...@df[@df.keys[0]].length
        match = true
        combo.keys.each {|col| match = false if @df[col][i] != combo[col]}
        if match
          # If they match, search for matches in the other DataFrame
          for j in 0...other_df.nrows
            other_match = true
            combo.keys.each {|col| other_match = false if other_df.select(col)[j] != combo[col]}
            if other_match
              # Enter the data they both share
              merge_cols.each {|col| new_df[col.to_s].push(combo[col])}
              # Enter the data from the calling DataFrame
              @df.keys.each do |col|
                if !combo.keys.include?(col) && !other_df.columns.include?(col)
                  new_df[col.to_s].push(@df[col][i])
                elsif !combo.keys.include?(col) && other_df.columns.include?(col)
                  new_df[col.to_s + rsuffix.to_s].push(@df[col][i])
                end
              end
              # Enter the data from the other DataFrame
              other_df.columns.each do |col|
                if !combo.keys.include?(col) && !@df.keys.include?(col)
                  new_df[col.to_s].push(other_df.select(col)[j])
                elsif !combo.keys.include?(col) && @df.keys.include?(col)
                  new_df[col.to_s + lsuffix.to_s].push(other_df.select(col)[j])
                end
              end
            end
          end
        end
      end
    end
    DataFrame.new(new_df)
  end

  # Appends the rows of the specified DataFrame to those of the calling DataFrame if the column names and data types match
  # By default, returns a new DataFrame with the rows appended
  def append(other_df, inplace: false)
    check_true_false(inplace)
    if @dtypes == other_df.dtypes(print_types: false)
      if inplace
        # Append the new rows to this DataFrame column-by-column
        other_df.columns.each {|col| @df[col] += other_df.select(col)}
        ''
      else
        # Create a new DataFrame, append the rows, and return
        data_arr = (0...@df[@df.keys[0]].length).to_a.each_with_object([]) do |i, data_arr|
          row = @df.keys.each_with_object({}) do |col, row|
            row[col] = @df[col][i]
          end
          data_arr.push(row)
        end
        data_arr += other_df.to_a
        DataFrame.new(data_arr)
      end
    else
      raise Exception.new("Column names and/or data types of DataFrame to append do not match those of the calling DataFrame.")
    end
  end

  # Write the DataFrame to a CSV file with the specified name
  def to_csv(file_name, sep: nil)
    if file_name.class != String
      raise Exception.new("Specified file name must be a string (#{file_name.class} object was input).")
    end
    if sep == nil
      CSV.open(file_name, 'w') do |csv|
        csv << @df.keys
        to_a.each {|row| csv << row.values}
      end
    else
      CSV.open(file_name, 'w', col_sep: sep) do |csv|
        csv << @df.keys
        to_a.each {|row| csv << row.values}
      end
    end
  end

end


# Returns a DataFrame containing the data from the specified CSV or other type of file
def read_csv(file_name, sep: nil)
  if file_name.class != String
    raise Exception.new("Specified file name must be a string (#{file_name.class} object was input).")
  end
  if sep == nil
    data_hash = {}
    firstline = true
    CSV.foreach(file_name) do |line|
      line.each {|col| data_hash[col] = []} if firstline
      data_hash.keys.each_with_index {|col, i| data_hash[col].push(line[i])} if !firstline
      firstline = false if firstline
    end
  else
    data_hash = {}
    firstline = true
    CSV.foreach(file_name, col_sep: sep) do |line|
      line.each {|col| data_hash[col] = []} if firstline
      data_hash.keys.each_with_index {|col, i| data_hash[col].push(line[i])} if !firstline
      firstline = false if firstline
    end
  end
  DataFrame.new(data_hash)
end

# Returns a DataFrame containing the combined contents of the DataFrames contained within the input hash
def concat(dataframes)
  if dataframes.class != Array
    raise Exception.new("Names of DataFrames to concatenate must be in an array (#{dataframes.class} object was input).")
  elsif dataframes.length < 2
    raise Exception.new("At least two DataFrames must be provided to concatenate (#{dataframes.length} provided).")
  else
    non_df_classes = dataframes.map {|dataframe| dataframe.class}.uniq - [DataFrame]
    if non_df_classes.length > 0
      raise Exception.new("Objects to concatenate must be DataFrames (#{non_df_classes.join(", ")} object(s) provided).")
    end
  end
  combined = dataframes[0]
  dataframes.shift
  dataframes.each {|dataframe| combined.append(dataframe, inplace: true)}
  combined
  # # Another slower, yet more elegant way of concatenating
  # DataFrame.new(dataframes.each_with_object([]) {|dataframe, arr| arr += dataframe.to_a})
end

# Ensure that the input is an array and that its values are all either Floats or Fixnums
def check_input_types(data)
  if data.class != Array
    raise Exception.new("Input must be an array (#{data.class} class detected).")
  end
  dtypes = data.each do |v|
    if ![Float, Fixnum].include?(v.class)
      raise Exception.new("Input array must only contain Float or Fixnum class values (#{v.class} class detected).")
    end
  end
end

# Calculate the mean of the data and return as a float
def avg(data)
  check_input_types(data)
  data.inject {|s, v| s += v}.to_f / data.length
end

# Calculate the standard deviation of the population
def std(data)
  mean = avg(data)
  n = data.length
  data.inject {|s, v| s += Math.sqrt((v - mean) ** 2 / n)}
end

# ex = {'name'=>['Brian', 'Andrew', 'Andrew', 'Andrew'], 'age'=>[13, 30, 24, 30], 'art'=>['jujutsu', 'wushu', 'taekwondo', 'kendo']}
# ex = DataFrame.new(ex)
# ex.disp

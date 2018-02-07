require 'minitest/autorun'
require_relative 'data_frames.rb'

df = read_csv('test1.csv')
df.nrows
df.ncols
df2 = read_csv('test2.csv')
df3 = df.merge(df2, ['participant'])

class TestDataFrame < Minitest::Test
  def setup
    @df = read_csv('test1.csv')
  end

  def test_check_rows
    assert_equal 5, @df.nrows
  end

  def test_check_columns
    assert_equal 4, @df.ncols
  end

  def test_shape
    assert_equal [5, 4], @df.shape
  end

  def col_name_arr

  end



end

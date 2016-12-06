package dmlab.main;

public interface Algorithm<Input,Output> {
    Output process(Input input, DoublePoint start);
}


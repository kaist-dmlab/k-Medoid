package dmlab.main;

public interface Algorithm<Input,Output> {
    Output process(Input input, FloatPoint start);
}


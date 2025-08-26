import matplotlib.pyplot as plt

def plot_simulations(results):
    for sim in results:
        plt.plot(sim)
    plt.title("Monte Carlo Simulations")
    plt.xlabel("Days")
    plt.ylabel("Price")
    plt.show()
